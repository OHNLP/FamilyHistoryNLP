//package org.ohnlp.familyhistory.subtasks;
//
//import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
//import org.apache.beam.sdk.coders.KvCoder;
//import org.apache.beam.sdk.coders.RowCoder;
//import org.apache.beam.sdk.coders.StringUtf8Coder;
//import org.apache.beam.sdk.schemas.Schema;
//import org.apache.beam.sdk.schemas.transforms.Join;
//import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.GroupByKey;
//import org.apache.beam.sdk.transforms.PTransform;
//import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.values.*;
//import org.ohnlp.familyhistory.tasks.ExtractEligibleEntities;
//
//import java.util.regex.Pattern;
//
///**
// * For sentence clauses containing a referential pronoun (e.g., he, she, none of them, his, her),
// * backtracks sentence/chunk ID by one (i.e., to the previous subclause) and searches for a family member
// * mention there.
// */
//public class GenerateCandidatePairsCrossSentenceChunk extends PTransform<PCollectionTuple, PCollection<Row>> {
//    public static TupleTag<Row> ANNOTATED_SENTENCES_TUPLE_TAG = new TupleTag<>(){};
//    public static TupleTag<Row> CHUNKS_BY_SENTENCE_ID_TUPLE_TAG = new TupleTag<>(){};
//    public static TupleTag<Row> EXTRACTED_ENTITIES_TUPLE_TAG = new TupleTag<>(){};
//
//
//    @Override
//    public PCollection<Row> expand(PCollectionTuple input) {
//        PCollection<Row> annotatedSentences = input.get(ANNOTATED_SENTENCES_TUPLE_TAG);
//        PCollection<Row> chunks_by_sentence_id = input.get(CHUNKS_BY_SENTENCE_ID_TUPLE_TAG);
//        PCollection<Row> entities = input.get(EXTRACTED_ENTITIES_TUPLE_TAG);
//        // - First determine correct sentence index for joining purposes
//        Schema crossReferenceableEntity = Schema.of(
//                Schema.Field.of("document_id", Schema.FieldType.STRING),
//                Schema.Field.of("sentence_id", Schema.FieldType.INT32),
//                Schema.Field.of("chunk_id", Schema.FieldType.INT32),
//                Schema.Field.of("concept", Schema.FieldType.STRING),
//                Schema.Field.of("modifier", Schema.FieldType.STRING)
//        );
//        PCollection<Row> observationsByTargetPrecedingChunk =
//                annotatedSentences.apply("Filter Sentences that Need to be Cross-Referenced",
//                        ParDo.of(new DoFn<Row, Row>() {
//                            private Pattern referentialPronouns;
//
//                            @StartBundle
//                            public void init() {
//                                this.referentialPronouns = Pattern.compile("He|She|None of them|her|his", Pattern.CASE_INSENSITIVE);
//                            }
//
//                            @ProcessElement
//                            public void process(ProcessContext pc) {
//                                Row r = pc.element();
//                                assert r != null;
//                                String baseSentence = r.getString("annotated_sentence");
//                                // TODO this is suboptimal (he .... but his mother .... is not supported here) but that is how
//                                // TODO it is implemented in perl, look at trying to solve with dependency parsing in the future
//                                assert baseSentence != null;
//                                if (referentialPronouns.matcher(baseSentence).find() && !baseSentence.contains("_degree")) {
//                                    pc.output(r);
//                                }
//                            }
//                        })
//                ).setCoder(RowCoder.of(AnnotateEntitiesInSentences.SCHEMA)).apply(
//                        "Join with Entities to get Observation Information",
//                        Join.<Row, Row>innerJoin(entities.apply(
//                                ParDo.of(new DoFn<Row, Row>() {
//                                    @ProcessElement
//                                    public void process(ProcessContext pc) {
//                                        if (pc.element().getString("entity_type").equals("Observation")) {
//                                            pc.output(pc.element());
//                                        }
//                                    }
//                                })
//                        ).setCoder(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA))).using("document_id", "sentence_id", "chunk_id")
//                ).apply(
//                        "Remap sentence/chunk IDs to preceding chunk",
//                        ParDo.of(new DoFn<Row, Row>() {
//
//                            @ProcessElement
//                            public void process(ProcessContext pc) {
//                                Row r = pc.element().getRow("rhs"); // We can completely ignore lhs as we dont care about the annotated sentence
//                                String docID = r.getString("document_id");
//                                int sentID = r.getInt32("sentence_id");
//                                int chunkID = r.getInt32("chunk_id");
//                                if (chunkID > 0) {
//                                    chunkID = chunkID - 1;
//                                } else {
//                                    sentID = sentID - 1;
//                                    chunkID = -1;
//                                }
//                                pc.output(Row.withSchema(crossReferenceableEntity).addValues(
//                                        docID, sentID, chunkID, r.getString("concept"), r.getString("modifier")
//                                ).build());
//                            }
//                        })
//                ).setCoder(RowCoder.of(crossReferenceableEntity));
//        // - Next join with chunks_by_sentence_id to get correct chunk_ids for chunk_id = -1
//        PCollection<Row> joinedTgt = observationsByTargetPrecedingChunk.apply("Get correct last chunk IDs",
//                Join.<Row, Row>innerJoin(chunks_by_sentence_id)
//                        .on(Join.FieldsEqual.left("document_id", "sentence_id").right("document_id", "cleaned_sentence_id")));
//        // - Compress and Map back to crossReferenceableEntitySchema
//        joinedTgt = joinedTgt.apply(ParDo.of(new DoFn<Row, Row>() {
//            @ProcessElement
//            public void process(ProcessContext pc) {
//                Row r = pc.element();
//                Row data = r.getRow("lhs");
//                Row chunkInfo = r.getRow("rhs");
//                if (data.getInt32("chunk_id") != -1) {
//                    // This was a chunk_id decrement within the same sentence overall, so no lookup of top chunk_id of preceding sentence needed
//                    pc.output(data);
//                } else {
//                    int topChunk = chunkInfo.getInt32("num_chunks") - 1;
//                    pc.output(Row.fromRow(data).withFieldValue("chunk_id", topChunk).build());
//                }
//            }
//        })).setCoder(RowCoder.of(crossReferenceableEntity));
//        // - Get last family member mention by sentence
//        PCollection<Row> lastFHMentionsByChunk = entities.apply(ParDo.of(new DoFn<Row, KV<KV<String, KV<Integer, Integer>>, KV<Integer, Row>>>() {
//            // Map to <(doc_id, sent_id, chunk_id), (entity_sequence_number, row)>
//            @ProcessElement
//            public void process(ProcessContext pc) {
//                Row r = pc.element();
//                if (!r.getString("entity_type").equals("FamilyMember")) {
//                    return;
//                }
//                KV<String, KV<Integer, Integer>> key = KV.of(
//                        r.getString("document_id"),
//                        KV.of(
//                                r.getInt32("sentence_id"),
//                                r.getInt32("chunk_id")
//                        )
//                );
//                KV<Integer, Row> row = KV.of(
//                        r.getInt32("entity_sequence_number"),
//                        r
//                );
//                pc.output(KV.of(key, row));
//            }
//        })).setCoder(
//                KvCoder.of(
//                        KvCoder.of(StringUtf8Coder.of(),
//                                KvCoder.of(
//                                        BigEndianIntegerCoder.of(),
//                                        BigEndianIntegerCoder.of()
//                                )
//                        ),
//                        KvCoder.of(
//                                BigEndianIntegerCoder.of(),
//                                RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA)
//                        )
//                )).apply(
//                GroupByKey.create()
//        ).apply("Retain row with highest entity_sequence_number per group only",
//                ParDo.of(new DoFn<KV<KV<String, KV<Integer, Integer>>, Iterable<KV<Integer, Row>>>, Row>() {
//                    @ProcessElement
//                    public void process(ProcessContext pc) {
//                        KV<KV<String, KV<Integer, Integer>>, Iterable<KV<Integer, Row>>> e = pc.element();
//                        int max_entity_sequence = -1;
//                        Row maxRow = null;
//                        for (KV<Integer, Row> r : e.getValue()) {
//                            if (r.getKey() > max_entity_sequence) {
//                                max_entity_sequence = r.getKey();
//                                maxRow = r.getValue();
//                            }
//                        }
//                        if (maxRow != null) {
//                            pc.output(maxRow);
//                        }
//                    }
//                })
//        ).setCoder(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA));
//        // - Now join with joinedTgt and write out pairs
//        return joinedTgt.apply("Join entities with referential pronoun relations to previous sentence chunk",
//                Join.<Row, Row>innerJoin(lastFHMentionsByChunk).using(
//                        "document_id", "sentence_id", "chunk_id"
//                )).apply(ParDo.of(new DoFn<Row, Row>() {
//            @ProcessElement
//            public void process(ProcessContext pc) {
//                Row entity = pc.element().getRow("lhs");
//                Row fmInfo = pc.element().getRow("rhs");
//                pc.output(
//                        Row.withSchema(FHPostprocessTransformReln.SCHEMA)
//                                .addValues(
//                                        entity.getString("document_id"),
//                                        fmInfo.getString("concept"),
//                                        fmInfo.getString("modifier"),
//                                        "Observation",
//                                        entity.getString("concept"),
//                                        entity.getString("modifier")
//                                ).build());
//            }
//        })).setCoder(
//                RowCoder.of(FHPostprocessTransformReln.SCHEMA)
//        );
//    }
//}
