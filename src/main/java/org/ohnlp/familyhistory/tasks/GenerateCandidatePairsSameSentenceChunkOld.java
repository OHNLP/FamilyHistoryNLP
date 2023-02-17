package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Produces cartesian product of finding/disorder and family member mentions within the same sentence chunk
 */
public class GenerateCandidatePairsSameSentenceChunkOld extends DoFn<Row, Row> {

    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("chunk_id", Schema.FieldType.INT32),
            Schema.Field.of("annotated_sentence", Schema.FieldType.STRING),
            Schema.Field.of("family_member", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("side", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("clinical_entity", Schema.FieldType.STRING.withNullable(true))
    );
    private Pattern FM_PATTERN;
    private Pattern DISO_PATTERN;

    @StartBundle
    public void init() {
        this.FM_PATTERN = Pattern.compile("\"null\":\"\\w{0,8}_degree_relative-([^\"]{1,12})\"");
        this.DISO_PATTERN = Pattern.compile("\"DISO\":\"(.*)\"");
    }

    @ProcessElement
    public void process(ProcessContext pc) {
        Row row = pc.element();
        String sentence = row.getString("annotated_sentence").replaceAll("FIND", "DISO");
        if (!sentence.contains("DISO")) {
            return;
        }
        Matcher m = FM_PATTERN.matcher(sentence);
        ArrayList<String> familyMembers = new ArrayList<>();
        while (m.find()) {
            familyMembers.add(m.group(1));
        }
        ArrayList<String> disos = new ArrayList<>();
        m = DISO_PATTERN.matcher(sentence);
        while (m.find()) {
            disos.add(m.group(1));
        }
        for (String fm : familyMembers) {
            for (String d : disos) {
                pc.output(Row.withSchema(SCHEMA).addValues(
                        row.getString("document_id"),
                        row.getInt32("sentence_id"),
                        row.getInt32("chunk_id"),
                        sentence,
                        fm,
                        d
                ).build());
            }
        }
    }
}
