package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;
import org.ohnlp.familyhistory.subtasks.AnnotateEntitiesInSentences;
import org.ohnlp.familyhistory.subtasks.ConstituencyParseSentences;
import org.ohnlp.familyhistory.subtasks.ExtractAndClassifyEntities;
import org.ohnlp.familyhistory.subtasks.IdentifyEntitiesToRemoveByContext;

import java.util.stream.StreamSupport;

/**
 * Extracts Entities Eligible for FH Relation Assignment
 * <br/>
 * Output has several accessor tags:
 * For extracted entities:
 * <ul>
 *     <li>{@link #CLINICAL_ENTITY_TAG} for a PCollection of clinical entities</li>
 *     <li>{@link #FAMILY_MEMBER_TAG} for a PCollection of family members</li>
 *     <li>{@link #ALL_ENTITIES_TAG} for all entities</li>
 * </ul>
 * <br/>
 * Output format is (document_id, sentence_id, chunk_id, entity_type, concept, modifier, entity_sequence_number)
 */
public class ExtractEligibleEntities extends PTransform<PCollectionTuple, PCollectionTuple> {

    public static Schema ENTITY_SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("chunk_id", Schema.FieldType.INT32),
            Schema.Field.of("entity_type", Schema.FieldType.STRING),
            Schema.Field.of("concept", Schema.FieldType.STRING),
            Schema.Field.of("modifier", Schema.FieldType.STRING).withNullable(true),
            Schema.Field.of("entity_sequence_number", Schema.FieldType.INT32)
    );

    public static Schema ANNOTATED_SENTENCE_SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("chunk_id", Schema.FieldType.INT32),
            Schema.Field.of("base_sentence", Schema.FieldType.STRING),
            Schema.Field.of("annotated_sentence", Schema.FieldType.STRING)
    );

    public static TupleTag<Row> CLINICAL_ENTITY_TAG = new TupleTag<>() {};
    public static TupleTag<Row> FAMILY_MEMBER_TAG = new TupleTag<>() {};
    public static TupleTag<Row> ALL_ENTITIES_TAG = new TupleTag<>() {};
    public static TupleTag<Row> ANNOTATED_SENTENCES_TAG = new TupleTag<>() {};

    @Override
    public PCollectionTuple expand(PCollectionTuple segmentInputSentences) {
        PCollection<Row> input = segmentInputSentences.get(SegmentInputSentences.MAIN_OUTPUT_TAG);
        // pl task1_MedTagger_result_output_1 equivalent
        PCollection<Row> entities =
                input.apply(
                        "Extract and Classify Entities (getTask1.pl)",
                        ParDo.of(new ExtractAndClassifyEntities())).setCoder(RowCoder.of(ENTITY_SCHEMA)
                ).apply("Select Distinct", Distinct.create()).setCoder(RowCoder.of(ENTITY_SCHEMA));
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
        })).setCoder(KvCoder.of(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()), RowCoder.of(ENTITY_SCHEMA)));
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
        PCollectionTuple out = KeyedPCollectionTuple.of(t1, groupbaleEntities).and(t2, groupableEntitiesToRemove)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<KV<String, String>, CoGbkResult>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        CoGbkResult group = pc.element().getValue();
                        System.out.println(group.getAll(t2));
                        if (!StreamSupport.stream(group.getAll(t2).spliterator(), false).findAny().isPresent()) {
                            group.getAll(t1).forEach(r -> {
                                if (r.getString("entity_type").equals("Observation")) {
                                    pc.output(CLINICAL_ENTITY_TAG, r);
                                } else {
                                    pc.output(FAMILY_MEMBER_TAG, r);
                                }
                                pc.output(ALL_ENTITIES_TAG, r);
                            });
                        }
                    }
                }).withOutputTags(CLINICAL_ENTITY_TAG, TupleTagList.of(FAMILY_MEMBER_TAG).and(ALL_ENTITIES_TAG)));
        return PCollectionTuple.of(CLINICAL_ENTITY_TAG, out.get(CLINICAL_ENTITY_TAG).setRowSchema(ENTITY_SCHEMA))
                .and(FAMILY_MEMBER_TAG, out.get(FAMILY_MEMBER_TAG).setRowSchema(ENTITY_SCHEMA))
                .and(ALL_ENTITIES_TAG, out.get(ALL_ENTITIES_TAG).setRowSchema(ENTITY_SCHEMA))
                .and(ANNOTATED_SENTENCES_TAG, annotatedSentences);
    }
}
