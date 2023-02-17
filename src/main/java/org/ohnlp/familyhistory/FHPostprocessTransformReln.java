package org.ohnlp.familyhistory;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.familyhistory.subtasks.*;
import org.ohnlp.familyhistory.tasks.ExtractEligibleEntities;
import org.ohnlp.familyhistory.tasks.SegmentInputSentences;

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
        PCollectionTuple sentenceSegments = input.apply(new SegmentInputSentences());
        PCollectionTuple extractedEntities = sentenceSegments.apply(new ExtractEligibleEntities());
        // TODO included for completeness, migrate cross join in new implementation to no longer use this
        PCollection<Row> entities = extractedEntities.get(ExtractEligibleEntities.ALL_ENTITIES_TAG).setCoder(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA));
        PCollection<Row> family_members = extractedEntities.get(ExtractEligibleEntities.FAMILY_MEMBER_TAG).setCoder(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA));
        PCollection<Row> clinical_entities = extractedEntities.get(ExtractEligibleEntities.CLINICAL_ENTITY_TAG).setCoder(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA));
        // pl task2_output_1 equivalent
        // Do a cartesian product for all family members and clinical entities in the same sentence chunk
        PCollection<Row> candidatePairsSameConstituentChunk = family_members.apply(
                Join.<Row, Row>innerJoin(clinical_entities).using("document_id", "sentence_id", "chunk_id")
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
                extractedEntities.get(ExtractEligibleEntities.ANNOTATED_SENTENCES_TAG)
                        .setCoder(RowCoder.of(ExtractEligibleEntities.ANNOTATED_SENTENCE_SCHEMA))
        ).and(
                GenerateCandidatePairsCrossSentenceChunk.CHUNKS_BY_SENTENCE_ID_TUPLE_TAG,
                sentenceSegments.get(SegmentInputSentences.CHUNK_COUNT_TAG)
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
