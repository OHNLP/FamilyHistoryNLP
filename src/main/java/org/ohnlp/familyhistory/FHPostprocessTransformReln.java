package org.ohnlp.familyhistory;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.familyhistory.tasks.*;

import java.util.stream.StreamSupport;

public class FHPostprocessTransformReln extends Transform {

    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("family_member", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("side", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("type", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("clinical_entity", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("certainty", Schema.FieldType.STRING.withNullable(true))

    );

    // A Java Implementation of the original FH perl code in Beam PTransform form with added preprocessing in the form
    // of constituency parse segmentation

    @Override
    public void initFromConfig(JsonNode jsonNode) throws ComponentInitializationException {
        // Do nothing
    }

    @Override
    public Schema calculateOutputSchema(Schema schema) {
        return SCHEMA;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        // First run Stanford CoreNLP Constituency Parse to Break up Sentences into Constituent Clauses
        ConstituencyParseSentences constituencyParseDoFn = new ConstituencyParseSentences();
        Schema s = constituencyParseDoFn.initSchema(input.getSchema());
        PCollectionTuple multiCollectionOutput = input.apply(
                "Constituency Parse and Clean Sentence IDs",
                ParDo.of(constituencyParseDoFn).withOutputTags(ConstituencyParseSentences.MAIN_OUTPUT_TAG, TupleTagList.of(ConstituencyParseSentences.CHUNK_COUNT_TAG)));
        input = multiCollectionOutput.get(ConstituencyParseSentences.MAIN_OUTPUT_TAG).setCoder(RowCoder.of(s));
        PCollection<Row> chunks_by_sentence_id = multiCollectionOutput.get(ConstituencyParseSentences.CHUNK_COUNT_TAG).setCoder(RowCoder.of(ConstituencyParseSentences.CHUNK_COUNT_SCHEMA));
        // pl task1_MedTagger_result_output_1 equivalent
        PCollection<Row> entities =
                input.apply(
                        "Extract and Classify Entities (getTask1.pl)",
                        ParDo.of(new ExtractAndClassifyEntities())).setCoder(RowCoder.of(ExtractAndClassifyEntities.SCHEMA)
                ).apply("Select Distinct", Distinct.create()).setCoder(RowCoder.of(ExtractAndClassifyEntities.SCHEMA));
        PCollection<Row> annotatedSentences = input.apply(
                "Annotate Sentences with Entities", //pl task1_MedTagger_result_output_2 equivalent
                new AnnotateEntitiesInSentences()
        ).setCoder(RowCoder.of(AnnotateEntitiesInSentences.SCHEMA));
        PCollection<Row> entitiesToRemove = annotatedSentences.apply("Identify entities to remove by context", // pl task1_MedTagger_result_output_3 equivalent
                ParDo.of(new IdentifyEntitiesToRemoveByContext())
        ).setCoder(RowCoder.of(IdentifyEntitiesToRemoveByContext.SCHEMA));
        // Convert to KV pairs keyed by (document_id, {family_member|observation_matched_text}), and output only if same key does not exist in entitiesToRemove
        PCollection<KV<KV<String, String>, Row>> groupbaleEntities = entities.apply(ParDo.of(new DoFn<Row, KV<KV<String, String>, Row>>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                Row row = pc.element();
                pc.output(KV.of(KV.of(row.getString("document_id"), row.getString("concept")), row));
            }
        })).setCoder(KvCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), RowCoder.of(ExtractAndClassifyEntities.SCHEMA)));
        PCollection<KV<KV<String, String>, Integer>> groupableEntitiesToRemove = entitiesToRemove.apply(ParDo.of(new DoFn<Row, KV<KV<String, String>, Integer>>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                Row row = pc.element();
                pc.output(KV.of(KV.of(row.getString("document_id"), row.getString("entity")), 1));
            }
        }));
        final TupleTag<Row> t1 = new TupleTag<>();
        final TupleTag<Integer> t2 = new TupleTag<>();
        // pl task1_MedTagger_result_output_4 equivalent
        entities = KeyedPCollectionTuple.of(t1, groupbaleEntities).and(t2, groupableEntitiesToRemove)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<KV<String, String>, CoGbkResult>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        CoGbkResult group = pc.element().getValue();
                        System.out.println(group.getAll(t2));
                        if (!StreamSupport.stream(group.getAll(t2).spliterator(), false).findAny().isPresent()) {
                            group.getAll(t1).forEach(pc::output);
                        }
                    }
                })).setCoder(RowCoder.of(ExtractAndClassifyEntities.SCHEMA));
        // pl task2_output_1 equivalent
        PCollection<Row> candidatePairsSameConstituentChunk = entities.apply(
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        if (pc.element().getString("entity_type").equals("FamilyMember")) {
                            pc.output(pc.element());
                        }
                    }
                })
        ).setCoder(RowCoder.of(ExtractAndClassifyEntities.SCHEMA)).apply(
                Join.<Row, Row>innerJoin(
                        entities.apply(
                                ParDo.of(new DoFn<Row, Row>() {
                                    @ProcessElement
                                    public void process(ProcessContext pc) {
                                        if (pc.element().getString("entity_type").equals("Observation")) {
                                            pc.output(pc.element());
                                        }
                                    }
                                })
                        ).setCoder(RowCoder.of(ExtractAndClassifyEntities.SCHEMA))
                ).using("document_id", "sentence_id", "chunk_id")
        ).apply(
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row faminfo = pc.element().getRow("lhs");
                        Row obsinfo = pc.element().getRow("rhs");
                        pc.output(
                            Row.withSchema(SCHEMA).addValues(
                                    faminfo.getString("document_id"),
                                    faminfo.getString("concept"),
                                    faminfo.getString("modifier"),
                                    "Observation",
                                    obsinfo.getString("concept"),
                                    obsinfo.getString("modifier")
                            ).build()
                        );
                    }
                })
        ).setCoder(RowCoder.of(SCHEMA));
        // Generate pl task2_output_2 equivalent
        PCollectionTuple crossReferentialInput = PCollectionTuple.of(
                GenerateCandidatePairsCrossSentenceChunk.ANNOTATED_SENTENCES_TUPLE_TAG,
                annotatedSentences
        ).and(
                GenerateCandidatePairsCrossSentenceChunk.CHUNKS_BY_SENTENCE_ID_TUPLE_TAG,
                chunks_by_sentence_id
        ).and(
                GenerateCandidatePairsCrossSentenceChunk.EXTRACTED_ENTITIES_TUPLE_TAG,
                entities
        );
        PCollection<Row> candidatePairsConsecutiveSentenceChunk = crossReferentialInput.apply(
                "Generate cross-sentence chunk relation pairs",
                new GenerateCandidatePairsCrossSentenceChunk()
        ).setCoder(RowCoder.of(SCHEMA));
        return PCollectionList.of(candidatePairsSameConstituentChunk).and(candidatePairsConsecutiveSentenceChunk).apply(Flatten.pCollections())
                .apply(Distinct.create());
    }
}
