package org.ohnlp.familyhistory.subtasks;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * This component performs segmentation, if desired. At present, this functionality is not present and as such only
 * re-outputs sentences in order
 */
public class SegmentSentenceViaConstituencyParse extends PTransform<PCollection<Row>, PCollection<Row>> {

    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("chunk_id", Schema.FieldType.INT32),
            Schema.Field.of("sequenced_chunk_id_in_document", Schema.FieldType.INT32),
            Schema.Field.of("chunk_text", Schema.FieldType.STRING),
            Schema.Field.of("chunk_start_offset", Schema.FieldType.INT32),
            Schema.Field.of("chunk_end_offset", Schema.FieldType.INT32)
    );

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply(
                "Map to (document_id, row) tuples",
                ParDo.of(new DoFn<Row, KV<String, Row>>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row r = pc.element();
                        pc.output(KV.of(r.getString("document_id"), r));
                    }
                })
        ).setCoder(
                KvCoder.of(StringUtf8Coder.of(), RowCoder.of(DeduplicateAndCleanExtractedSentences.SCHEMA))
        ).apply(
                "Group by Document ID",
                GroupByKey.create()
        ).apply(
                "Generate Segments in Order By Document",
                ParDo.of(new DoFn<KV<String, Iterable<Row>>, Row>() {
                    @StartBundle
                    public void init() {
                    }

                    @ProcessElement
                    public void process(ProcessContext pc) {
                        String docID = pc.element().getKey();
                        Iterable<Row> sentences = pc.element().getValue();
                        ArrayList<Row> cleanedRows = new ArrayList<>();
                        sentences.forEach(r -> {
                            cleanedRows.add(
                                    Row.withSchema(SCHEMA).addValues(
                                            docID,
                                            r.getInt32("sentence_id"),
                                            -1,
                                            -1,
                                            r.getString("matched_sentence"),
                                            -1,
                                            -1
                                    ).build()
                            );
                        });
                        cleanedRows.sort(Comparator.comparingInt(r -> r.getInt32("sentence_id")));
                        // Now do actual segmentation
                        int documentChunkCount = 0;
                        for (Row r : cleanedRows) {
                            pc.output(
                                    Row.withSchema(SCHEMA).addValues(
                                            r.getString("document_id"),
                                            r.getInt32("sentence_id"),
                                            0,
                                            documentChunkCount++,
                                            r.getString("chunk_text"),
                                            0,
                                            r.getString("chunk_text").length()
                                    ).build()
                            );
                        }
                    }
                })
        );
    }
}
