package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CleanMedTaggerOutput extends PTransform<PCollection<Row>, PCollection<Row>> {

    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("matched_text", Schema.FieldType.STRING),
            Schema.Field.of("concept_code", Schema.FieldType.STRING),
            Schema.Field.of("matched_sentence", Schema.FieldType.STRING),
            Schema.Field.of("certainty", Schema.FieldType.STRING),
            Schema.Field.of("offset", Schema.FieldType.INT32),
            Schema.Field.of("sent_offset", Schema.FieldType.INT32),
            Schema.Field.nullable("semgroups", Schema.FieldType.STRING)
    );

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply(ParDo.of(new DoFn<Row, Row>() {
            private Pattern sentenceIDCleanerPattern;

            @StartBundle
            public void init() {
                this.sentenceIDCleanerPattern = Pattern.compile("DocBegin:([-0-9]+)");
            }
            @ProcessElement
            public void process(ProcessContext pc) {
                Row r = pc.element();
                Matcher m = this.sentenceIDCleanerPattern.matcher(r.getString("sentid"));
                if (!m.find()) {
                    return;
                }
                int cleanedSentID = Integer.parseInt(m.group(1));
                pc.output(
                        Row.withSchema(SCHEMA).addValues(
                            r.getString("note_id"),
                                cleanedSentID,
                                r.getString("matched_text"),
                                r.getString("concept_code"),
                                r.getString("matched_sentence"),
                                r.getString("certainty"),
                                r.getInt32("offset"),
                                r.getInt32("sent_offset"),
                                r.getString("semgroups")
                ).build()
                );
            }
        })).setRowSchema(SCHEMA);
    }
}
