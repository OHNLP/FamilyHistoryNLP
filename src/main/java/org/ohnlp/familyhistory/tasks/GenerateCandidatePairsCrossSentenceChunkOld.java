package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.ohnlp.familyhistory.FHPostprocessTransformReln;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GenerateCandidatePairsCrossSentenceChunkOld extends PTransform<PCollectionTuple, PCollection<Row>> {
    public static TupleTag<Row> ANNOTATED_SENTENCES_TUPLE_TAG = new TupleTag<>();
    public static TupleTag<Row> CHUNKS_BY_SENTENCE_ID_TUPLE_TAG = new TupleTag<>();
    public static TupleTag<Row> EXTRACTED_ENTITIES_TUPLE_TAG = new TupleTag<>();


    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
        PCollection<Row> annotatedSentences = input.get(ANNOTATED_SENTENCES_TUPLE_TAG);
        PCollection<Row> chunks_by_sentence_id = input.get(CHUNKS_BY_SENTENCE_ID_TUPLE_TAG);
        PCollection<Row> entities = input.get(EXTRACTED_ENTITIES_TUPLE_TAG);
        // - First determine correct sentence index for joining purposes
        Schema observableEntitySchema = Schema.of(
                Schema.Field.of("document_id", Schema.FieldType.STRING),
                Schema.Field.of("sentence_id", Schema.FieldType.INT32),
                Schema.Field.of("chunk_id", Schema.FieldType.INT32),
                Schema.Field.of("concept", Schema.FieldType.STRING));
        PCollection<Row> observationsByTargetPrecedingChunk =
                annotatedSentences.apply("Filter Sentences that Need to be Cross-Referenced",
                        ParDo.of(new DoFn<Row, Row>() {
                            private Pattern referentialPronouns;

                            @StartBundle
                            public void init() {
                                this.referentialPronouns = Pattern.compile("He|She|None of them|her|his", Pattern.CASE_INSENSITIVE);
                            }

                            @ProcessElement
                            public void process(ProcessContext pc) {
                                Row r = pc.element();
                                assert r != null;
                                String baseSentence = r.getString("annotated_sentence");
                                // TODO this is suboptimal (he .... but his mother .... is not supported here) but that is how
                                // TODO it is implemented in perl
                                assert baseSentence != null;
                                if (referentialPronouns.matcher(baseSentence).find() && !baseSentence.contains("_degree")) {
                                    pc.output(r);
                                }
                            }
                        })
                ).apply(
                        "Extract observations and output alongside preceding chunk target ids",
                        ParDo.of(new DoFn<Row, Row>() {
                            private Pattern DISO_PATTERN;

                            @StartBundle
                            public void init() {
                                this.DISO_PATTERN = Pattern.compile("\"DISO\":\"(.*)\"");
                            }

                            @ProcessElement
                            public void process(ProcessContext pc) {
                                Row r = pc.element();
                                String docID = r.getString("document_id");
                                int sentID = r.getInt32("sentence_id");
                                int chunkID = r.getInt32("chunk_id");
                                if (chunkID > 0) {
                                    chunkID = chunkID - 1;
                                } else {
                                    sentID = sentID - 1;
                                    chunkID = -1;
                                }
                                String sent = r.getString("annotated_sentence");
                                Matcher m = DISO_PATTERN.matcher(sent.trim());
                                while (m.find()) {
                                    String obs = m.group(1);
                                    pc.output(Row.withSchema(observableEntitySchema).addValues(docID, sentID, chunkID, obs).build());
                                }
                            }
                        })
                );
        // - Next join with chunks_by_sentence_id to get correct chunk_ids for chunk_id = -1
        PCollection<Row> joinedTgt = observationsByTargetPrecedingChunk.apply("Get correct last chunk IDs",
                Join.<Row, Row>innerJoin(chunks_by_sentence_id)
                        .on(Join.FieldsEqual.left("document_id", "sentence_id").right("document_id", "cleaned_sentence_id")));
        // - Compress and Map back to observableEntitySchema
        joinedTgt = joinedTgt.apply(ParDo.of(new DoFn<Row, Row>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                Row r = pc.element();
                Row data = r.getRow("lhs");
                Row chunkInfo = r.getRow("rhs");
                if (data.getInt32("chunk_id") != -1) {
                    // This was a chunk_id decrement within the same sentence overall, so no lookup of top chunk_id of preceding sentence needed
                    pc.output(data);
                } else {
                    int topChunk = chunkInfo.getInt32("num_chunks") - 1;
                    pc.output(Row.fromRow(data).withFieldValue("chunk_id", topChunk).build());
                }
            }
        }));
        // - Get last family member mention by sentence
        PCollection<Row> lastFHMentionsByChunk = entities.apply(ParDo.of(new DoFn<Row, KV<KV<String, KV<Integer, Integer>>, KV<Integer, Row>>>() {
            // Map to <(doc_id, sent_id, chunk_id), (entity_sequence_number, row)>
            @ProcessElement
            public void process(ProcessContext pc) {
                Row r = pc.element();
                if (!r.getString("entity_type").equals("FamilyMember")) {
                    return;
                }
                KV<String, KV<Integer, Integer>> key = KV.of(
                        r.getString("document_id"),
                        KV.of(
                                r.getInt32("sentence_id"),
                                r.getInt32("chunk_id")
                        )
                );
                KV<Integer, Row> row = KV.of(
                        r.getInt32("entity_sequence_number"),
                        r
                );
                pc.output(KV.of(key, row));
            }
        })).apply(
                GroupByKey.create()
        ).apply("Retain row with highest entity_sequence_number per group only",
                ParDo.of(new DoFn<KV<KV<String, KV<Integer, Integer>>, Iterable<KV<Integer, Row>>>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        KV<KV<String, KV<Integer, Integer>>, Iterable<KV<Integer, Row>>> e = pc.element();
                        int max_entity_sequence = -1;
                        Row maxRow = null;
                        for (KV<Integer, Row> r : e.getValue()) {
                            if (r.getKey() > max_entity_sequence) {
                                max_entity_sequence = r.getKey();
                                maxRow = r.getValue();
                            }
                        }
                        if (maxRow != null) {
                            pc.output(maxRow);
                        }
                    }
                })
        );
        // - Now join with joinedTgt and write out pairs
        return joinedTgt.apply("Join entities with referential pronoun relations to previous sentence chunk",
                Join.<Row, Row>innerJoin(lastFHMentionsByChunk).using(
                        "document_id", "sentence_id", "chunk_id"
                )).apply(ParDo.of(new DoFn<Row, Row>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                Row entity = pc.element().getRow("lhs");
                Row fmInfo = pc.element().getRow("rhs");
                pc.output(
                        Row.withSchema(FHPostprocessTransformReln.SCHEMA)
                                .addValues(
                                        entity.getString("document_id"),
                                        fmInfo.getString("concept"),
                                        fmInfo.getString("modifier"),
                                        "Observation",
                                        entity.getString("concept"),
                                        entity.getString("modifier")
//                                        entity.getString("document_id"),
//                                        entity.getInt32("sentence_id"),
//                                        entity.getInt32("chunk_id"),
//                                        fmInfo.getString("concept"),
//                                        entity.getString("concept")
                                ).build());
            }
        })).setCoder(
                RowCoder.of(FHPostprocessTransformReln.SCHEMA)
        );
//
//                .apply("Join to retrieve annotated sentences for cross-sentence relations",
//                Join.<Row, Row>innerJoin(annotatedSentences).using("document_id", "sentence_id", "chunk_id")
//        ).apply("Extract annotated sentence for preceding family history chunk to output",
//                ParDo.of(new DoFn<Row, Row>() {
//                    @ProcessElement
//                    public void process(ProcessContext pc) {
//                        Row r = pc.element();
//                        Row entityInfo = r.getRow("lhs");
//                        Row annSentence = r.getRow("rhs");
//                        pc.output(
//                                Row.withSchema(GenerateCandidatePairsSameSentenceChunkOld.SCHEMA).addValues(
//                                        entityInfo.getString("document_id"),
//                                        entityInfo.getInt32("sentence_id"),
//                                        entityInfo.getInt32("chunk_id"),
//                                        annSentence.getString("annotated_sentence"),
//                                        entityInfo.getString("family_member"),
//                                        entityInfo.getString("clinical_entity")
//                                ).build()
//                        );
//                    }
//                })
//        );
    }
}
