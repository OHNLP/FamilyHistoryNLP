package org.ohnlp.familyhistory.subtasks;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.familyhistory.tasks.ExtractEligibleEntities;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Categorizes NLP matches into Observations and FamilyHistory mentions.
 * <br/>
 * For observations, stores certainty information into the modifier field. For family members, determines if first or
 * second degree, extracts relevant entity name from the MedTagger output concept code, and if second degree assigns
 * maternal/paternal side information to the modifier field.
 */
public class ExtractAndClassifyEntities extends DoFn<Row, Row> {

    private Pattern EXCLUDED_TEXT_MATCHES;
    private Pattern FIRST_DEGREE_RELATIVES;
    private Pattern SECOND_DEGREE_RELATIVES;
    private Pattern SECOND_DEGREE_SIDE_MATCHER;


    @StartBundle
    public void init() {
        // Compile regexes here to prevent overhead from repeated compilations per record
        this.EXCLUDED_TEXT_MATCHES = Pattern.compile("\\bdied|alive|living|lives|deceased|die|passed\\b", Pattern.CASE_INSENSITIVE);
        this.FIRST_DEGREE_RELATIVES = Pattern.compile(
                "Father|Mother|Parent|Sister|Brother|Daughter|Son|Child|Sibling",
                Pattern.CASE_INSENSITIVE
        );
        this.SECOND_DEGREE_RELATIVES = Pattern.compile(
                "Grandmother|Grandfather|Grandparent|Cousin|Aunt|Uncle",
                Pattern.CASE_INSENSITIVE
        );
        this.SECOND_DEGREE_SIDE_MATCHER = Pattern.compile(
                "(paternal|maternal)(\\s+\\S+){0,1}\\s*(Grandmother|Grandfather|Grandparent|Cousin|Aunt|Uncle)",
                Pattern.CASE_INSENSITIVE
        );
    }

    @ProcessElement
    public void process(ProcessContext pc) {
        // Mapping function that runs once per row
        Row row = pc.element();
        String matched_text = Objects.requireNonNull(Objects.requireNonNull(row).getString("matched_text")).replaceAll("\"", "");
        if (this.EXCLUDED_TEXT_MATCHES.matcher(matched_text).find()) {
            return;
        }
        String sanitizedDocID = row.getString("note_id");
        if (Objects.requireNonNull(row.getString("concept_code")).toLowerCase(Locale.ROOT).contains("_degree")) {
            String[] split_concept_code = Objects.requireNonNull(row.getString("concept_code")).split("-");
            if (split_concept_code.length < 2) {
                throw new IllegalArgumentException("Unsplittable: " + row.getString("concept_code"));
            }
            String rel = split_concept_code[1];
            rel = rel.replaceAll("\"", "").replaceAll("Half_", "");
            rel = rel.substring(0, 1).toUpperCase() + rel.substring(1);
            if (this.SECOND_DEGREE_RELATIVES.matcher(rel).find()) {
                // TODO what about second degree relatives with no mention of side?
                // TODO double check make sure functionality matches
                String sentence = row.getString("sentence_chunk");
                Matcher m = SECOND_DEGREE_SIDE_MATCHER.matcher(Objects.requireNonNull(sentence));
                boolean sideFound = false;
                while (m.find()) {
                    sideFound = true;
                    String side = m.group(1);
                    side = side.substring(0, 1).toUpperCase() + side.substring(1);
                    pc.output(Row.withSchema(ExtractEligibleEntities.ENTITY_SCHEMA).addValues(
                            sanitizedDocID,
                            row.getInt32("cleaned_sentence_id"),
                            row.getInt32("constituent_chunk_idx"),
                            "FamilyMember",
                            rel,
                            side,
                            row.getInt32("offset")
                    ).build());
                }
                if (!sideFound) {
                    pc.output(Row.withSchema(ExtractEligibleEntities.ENTITY_SCHEMA).addValues(
                            sanitizedDocID,
                            row.getInt32("cleaned_sentence_id"),
                            row.getInt32("constituent_chunk_idx"),
                            "FamilyMember",
                            rel,  // TODO this doesn't seem accurate? Why are we not using group 3 instead? (for mentions of both first and second degree in same sentence)
                            "NA",
                            row.getInt32("offset")
                    ).build());
                }
            } else if (this.FIRST_DEGREE_RELATIVES.matcher(rel).find()) { // We do this second in an else because grandparents include equivalent parent word
                pc.output(Row.withSchema(ExtractEligibleEntities.ENTITY_SCHEMA).addValues(
                        sanitizedDocID,
                        row.getInt32("cleaned_sentence_id"),
                        row.getInt32("constituent_chunk_idx"),
                        "FamilyMember",
                        rel,
                        "NA",
                        row.getInt32("offset")
                ).build());
            }
        } else {
            pc.output(Row.withSchema(ExtractEligibleEntities.ENTITY_SCHEMA).addValues(
                    sanitizedDocID,
                    row.getInt32("cleaned_sentence_id"),
                    row.getInt32("constituent_chunk_idx"),
                    "Observation",
                    matched_text,
                    row.getString("certainty"),
                    row.getInt32("offset")).build());
        }
    }
}
