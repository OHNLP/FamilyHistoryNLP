package org.ohnlp.familyhistory;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.familyhistory.tasks.*;

/**
 * Outputs relation pairs determined from medtagger output
 */
public class FHPostprocessTransformReln extends Transform {

    @Override
    public void initFromConfig(JsonNode jsonNode) throws ComponentInitializationException {
        // Do nothing
    }

    @Override
    public Schema calculateOutputSchema(Schema schema) {
        return GenerateRelationPairs.SCHEMA;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        PCollection<Row> cleanedInput = input.apply(new CleanMedTaggerOutput()).setRowSchema(CleanMedTaggerOutput.SCHEMA);
        PCollection<Row> segments = cleanedInput
                .apply(new SegmentInputSentences()).setRowSchema(SegmentInputSentences.SCHEMA);
        PCollectionRowTuple associateAnnotationWithSegmentInput = PCollectionRowTuple.of(
                AssociateAnnotationWithSegment.SEGMENT_META_TAG,
                segments
        ).and(
                AssociateAnnotationWithSegment.ANNOTATIONS_TAG,
                cleanedInput
        );

        PCollectionTuple extractedEntities = associateAnnotationWithSegmentInput.apply(new AssociateAnnotationWithSegment())
                .apply(new ExtractEligibleEntities());
        return extractedEntities.apply(new GenerateRelationPairs());
    }
}
