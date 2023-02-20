package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Converts referential concepts such as he/she to the last family member mentioned prior
 * (up to {@link #MAX_CROSS_REFERENCE_SENTENCE_LENGTH} sentences before)
 */
public class ResolveReferentialFMHConcepts extends PTransform<PCollectionTuple, PCollection<Row>> {
    private static int MAX_CROSS_REFERENCE_SENTENCE_LENGTH = 2;

    private static final Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("family_member", Schema.FieldType.STRING),
            Schema.Field.of("modifier", Schema.FieldType.STRING),
            Schema.Field.of("start_sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("start_chunk_id", Schema.FieldType.INT32),
            Schema.Field.of("end_sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("end_chunk_id", Schema.FieldType.INT32)
    );

    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
        PCollection<Row> familyMembers = input.get(ExtractEligibleEntities.FAMILY_MEMBER_TAG);
        PCollection<Row> referentialPronouns = input.get(ExtractEligibleEntities.REFERENTIAL_TAG);
        PCollection<KV<String, Row>> fmKVPairs = familyMembers.apply(
                "Transform fm mentions to KV pairs preparatory to group by document",
                ParDo.of(new DoFn<Row, KV<String, Row>>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        pc.output(KV.of(pc.element().getString("document_id"), pc.element()));
                    }
                })
        ).setCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA)));

        // Get cross-referenceable spans
        PCollection<Row> spansCrossReferencable = fmKVPairs.apply(GroupByKey.create()
        ).apply(
                "Re-output family member associations by segment",
                ParDo.of(new DoFn<KV<String, Iterable<Row>>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        KV<String, Iterable<Row>> e = pc.element();
                        // First ensure we are traversing in order
                        Iterable<Row> fam = e.getValue();
                        List<Row> famMemberMentions = new ArrayList<>();
                        for (Row r : fam) {
                            famMemberMentions.add(r);
                        }
                        famMemberMentions.sort(Comparator.comparingInt((Row r) -> r.getInt32("sequenced_chunk_id_in_document")).thenComparingInt(r -> r.getInt32("entity_sequence_number")));
                        int startSentID = 0;
                        int startChunkID = 0;
                        int endSentID = Integer.MIN_VALUE;
                        int endChunkID;
                        boolean foundFirstInDoc = false;
                        String currConcept = "UNKNOWN/NOT YET SPECIFIED";
                        String currModifier = "NA";
                        for (Row r : famMemberMentions) {
                            int localSentenceID = r.getInt32("sentence_id");
                            int localChunkID = r.getInt32("chunk_id");
                            if (foundFirstInDoc) {
                                if (endSentID - startSentID > MAX_CROSS_REFERENCE_SENTENCE_LENGTH) {
                                    // Check if this exceeds cross-reference length. If it does, then truncate
                                    endSentID = startSentID + MAX_CROSS_REFERENCE_SENTENCE_LENGTH + 1;
                                    endChunkID = 0;
                                } else {
                                    endSentID = localSentenceID;
                                    endChunkID = localChunkID;
                                }
                                pc.output(Row.withSchema(SCHEMA).addValues(
                                        r.getString("document_id"),
                                        currConcept,
                                        currModifier,
                                        startSentID,
                                        startChunkID,
                                        endSentID,
                                        endChunkID
                                ).build());
                            } else {
                                foundFirstInDoc = true;
                            }
                            String type = r.getString("entity_type");
                            // If this annotation is a full family member mention, update with new concept and modifier
                            // Otherwise, this is referential and we just keep old concept/modifier and don't do anything further
                            if (type.equals("FamilyMember")) {
                                currConcept = r.getString("concept");
                                currModifier = r.getString("modifier");
                            }
                        }
                        if (foundFirstInDoc) {
                            // Output trailing span
                            pc.output(Row.withSchema(SCHEMA).addValues(
                                    e.getKey(),
                                    currConcept,
                                    currModifier,
                                    startSentID,
                                    startChunkID,
                                    startSentID + MAX_CROSS_REFERENCE_SENTENCE_LENGTH + 1,
                                    0
                            ).build());
                        }
                    }
                })
        ).setRowSchema(SCHEMA);
        // Now join against referentials to determine a final collection of referential pronouns replaced with the
        // family member they are purportedly referencing
        return referentialPronouns.apply(
                "Join extracted referential pronouns with family member contextual spans",
                Join.<Row, Row>innerJoin(spansCrossReferencable).using("document_id")
        ).apply(
                "Filter overlapping spans",
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row reference = pc.element().getRow("lhs");
                        Row spanMeta = pc.element().getRow("rhs");
                        int referenceSentID = reference.getInt32("sentence_id");
                        int referenceChunkID = reference.getInt32("chunk_id");
                        int spanStartSentenceID = spanMeta.getInt32("start_sentence_id");
                        int spanStartChunkID = spanMeta.getInt32("start_chunk_id");
                        int spanEndSentenceID = spanMeta.getInt32("end_sentence_id");
                        int spanEndChunkID = spanMeta.getInt32("end_chunk_id");
                        // First check if sentence is eligible
                        if (referenceSentID >= spanStartSentenceID
                                && (
                                (referenceSentID < spanEndSentenceID)
                                        || (referenceSentID == spanEndSentenceID && spanEndChunkID > 0))
                        ) {
                            // Now check if chunk is eligible
                            boolean inChunk = true;
                            // - It is only when we are on the same sentence as the bounds that we need to check chunks
                            if ((referenceSentID == spanStartSentenceID || referenceSentID == spanEndSentenceID)) {
                                if (referenceSentID == spanStartSentenceID && referenceChunkID < spanStartChunkID) {
                                    inChunk = false;
                                }
                                if (referenceSentID == spanEndSentenceID && referenceChunkID >= spanEndChunkID) {
                                    inChunk = false;
                                }
                            }
                            if (inChunk) {
                                pc.output(
                                        Row.fromRow(reference)
                                                .withFieldValue("entity_type", "FamilyMember")
                                                .withFieldValue("concept", spanMeta.getString("family_member"))
                                                .withFieldValue("modifier", spanMeta.getString("modifier"))
                                                .build()
                                );
                            }
                        }
                    }
                })
        ).setRowSchema(ExtractEligibleEntities.ENTITY_SCHEMA);
    }
}
