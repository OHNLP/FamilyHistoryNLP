package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

/**
 * Generates relation pairs, first by checking within the same sentence chunk, then by cross-referencing to same sentence
 * after resolving referential pronouns
 */
public class GenerateRelationPairs extends PTransform<PCollectionTuple, PCollection<Row>> {

    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("family_member", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("side", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("type", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("clinical_entity", Schema.FieldType.STRING.withNullable(true)),
            Schema.Field.of("certainty", Schema.FieldType.STRING.withNullable(true))
    );

    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
        PCollection<Row> family_members = input.get(ExtractEligibleEntities.FAMILY_MEMBER_TAG);
        PCollection<Row> clinical_entities = input.get(ExtractEligibleEntities.CLINICAL_ENTITY_TAG);
        // pl task2_output_1 equivalent - Do a cartesian product for all family members and clinical entities in the same sentence clause.
        // Simultaneously, output observations that have found a family member so that we can later remove for same-sentence
        // product
        TupleTag<Row> outputRelns = new TupleTag<>() {
        };
        TupleTag<Row> matchedObs = new TupleTag<>() {
        };
        PCollectionTuple candidatePairsSameConstituentChunk = family_members.apply(
                Join.<Row, Row>innerJoin(clinical_entities).using("document_id", "sentence_id", "chunk_id")
        ).apply(
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row faminfo = pc.element().getRow("lhs");
                        Row obsinfo = pc.element().getRow("rhs");
                        pc.output(outputRelns,
                                Row.withSchema(SCHEMA).addValues(
                                        faminfo.getString("document_id"),
                                        faminfo.getString("concept"),
                                        faminfo.getString("modifier"),
                                        "Observation",
                                        obsinfo.getString("concept"),
                                        obsinfo.getString("modifier")
                                ).build()
                        );
                        pc.output(matchedObs, obsinfo);
                    }
                }).withOutputTags(outputRelns, TupleTagList.of(matchedObs))
        );
        // Generate pl task2_output_2 equivalent.
        // - First, resolve referential concepts and replace with family member mentions, and union with existing
        family_members = PCollectionList.of(input.apply(new ResolveReferentialFMHConcepts()))
                .and(family_members).apply(Flatten.pCollections()).apply(Distinct.create())
                .setRowSchema(ExtractEligibleEntities.ENTITY_SCHEMA);
        // - Next, remove from output clinical entities those that have already found a reln in same chunk
        // We do this by mapping collections to (row, 1) kv pairs, and then co-grouping by key, and only outputting if
        // clinical_entities_kv has an output and matched_obs_kv does not
        TupleTag<Integer> t1 = new TupleTag<>() {
        };
        TupleTag<Integer> t2 = new TupleTag<>() {
        };
        PCollection<KV<Row, Integer>> clinical_entities_kv = clinical_entities.apply(ParDo.of(new DoFn<Row, KV<Row, Integer>>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                pc.output(KV.of(pc.element(), 1));
            }
        })).setCoder(KvCoder.of(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA), BigEndianIntegerCoder.of()));
        PCollection<KV<Row, Integer>> matched_obs_kv = candidatePairsSameConstituentChunk.get(matchedObs).setRowSchema(ExtractEligibleEntities.ENTITY_SCHEMA).apply(ParDo.of(new DoFn<Row, KV<Row, Integer>>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                pc.output(KV.of(pc.element(), 1));
            }
        })).setCoder(KvCoder.of(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA), BigEndianIntegerCoder.of()));
        clinical_entities = KeyedPCollectionTuple.of(t1, clinical_entities_kv).and(t2, matched_obs_kv).apply(
                CoGroupByKey.create()
        ).apply(
                ParDo.of(new DoFn<KV<Row, CoGbkResult>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Iterable<Integer> entities = pc.element().getValue().getAll(t1);
                        Iterable<Integer> matched = pc.element().getValue().getAll(t2);
                        if (entities.iterator().hasNext() && !matched.iterator().hasNext()) {
                            pc.output(pc.element().getKey());
                        }
                    }
                })
        ).setRowSchema(ExtractEligibleEntities.ENTITY_SCHEMA);
        // - Finally, join by sentence ID, completely ignoring chunks
        PCollection<Row> candidatePairsSameSentenceWithCrossSentenceReferences = family_members.apply(
                Join.<Row, Row>innerJoin(clinical_entities).using("document_id", "sentence_id")
        ).apply(
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row faminfo = pc.element().getRow("lhs");
                        Row obsinfo = pc.element().getRow("rhs");
                        pc.output(Row.withSchema(SCHEMA).addValues(
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
        ).setRowSchema(SCHEMA);


        return PCollectionList.of(candidatePairsSameConstituentChunk.get(outputRelns).setRowSchema(SCHEMA))
                .and(candidatePairsSameSentenceWithCrossSentenceReferences)
                .apply(Flatten.pCollections())
                .apply(Distinct.create());
    }
}
