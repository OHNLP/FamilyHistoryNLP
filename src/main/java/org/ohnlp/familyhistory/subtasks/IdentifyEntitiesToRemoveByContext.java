package org.ohnlp.familyhistory.subtasks;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Marks multi-member (person who has|had ...another person) and annotations associated with
 * husbands/patients partner/wifes for removal
 */
public class IdentifyEntitiesToRemoveByContext extends DoFn<Row, Row> {
    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("entity", Schema.FieldType.STRING));
    private Pattern MULTI_DEGREE_PATTERN;
    private Pattern DEGREE_RELATIVE_MENTION_PATTERN;

    @StartBundle
    public void init() {
        MULTI_DEGREE_PATTERN = Pattern.compile("_degree_relative-\\w{1,10}\"\\s*(who)? (has|had)(\\s+\\S+){0,12}\\s*\"\\w{0,10}_degree_relative", Pattern.CASE_INSENSITIVE);
        DEGREE_RELATIVE_MENTION_PATTERN = Pattern.compile("_degree_relative-(\\w{1,12})", Pattern.CASE_INSENSITIVE);
    }

    @ProcessElement
    public void process(ProcessContext pc) {
        String docID = pc.element().getString("document_id");
        String sentence = pc.element().getString("annotated_sentence");
        int firstInstance = sentence.toLowerCase(Locale.ROOT).indexOf("_degree");
        if (firstInstance >= 0 && firstInstance != sentence.toLowerCase(Locale.ROOT).lastIndexOf("_degree")) { // occurs more than once
            Matcher m = MULTI_DEGREE_PATTERN.matcher(sentence);
            while (m.find()) {
                String[] substrings = sentence.split("_degree_relative-"); // equivalent to @f
                String entity = substrings[2].split("\"")[0];
                pc.output(Row.withSchema(SCHEMA).addValues(docID, entity).build());
            }
        }
        // If any of the three of these are in the sentence then exclude all family members from that sentence
        if (sentence.toLowerCase(Locale.ROOT).contains("husbands") ||
                sentence.toLowerCase(Locale.ROOT).contains("patients partner") ||
                sentence.toLowerCase(Locale.ROOT).contains("wifes")) {
                Matcher m = DEGREE_RELATIVE_MENTION_PATTERN.matcher(sentence);
                while (m.find()) {
                    pc.output(Row.withSchema(SCHEMA).addValues(docID, m.group(1)).build());
                }
        }
    }
}
