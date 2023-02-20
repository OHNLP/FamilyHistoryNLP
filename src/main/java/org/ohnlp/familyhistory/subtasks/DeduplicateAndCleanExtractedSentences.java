package org.ohnlp.familyhistory.subtasks;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Deduplicates sentences so that we only run constituency parse once on each sentence
 */
public class DeduplicateAndCleanExtractedSentences extends PTransform<PCollection<Row>, PCollection<Row>> {
    public static final Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("matched_sentence", Schema.FieldType.STRING)
    );
    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply(
                "Filter to only (note_id, sentid, matched_sentence)",
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row r = pc.element();
                        pc.output(Row.withSchema(SCHEMA).addValues(
                                r.getString("document_id"),
                                r.getInt32("sentence_id"),
                                r.getString("matched_sentence")).build());
                    }
                })
        ).setRowSchema(SCHEMA).apply("Get distinct sentences w/ identifiers",
                Distinct.create()
        ).setRowSchema(SCHEMA);
    }
}
