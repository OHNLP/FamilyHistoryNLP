package org.ohnlp.familyhistory;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.familyhistory.tasks.AssociateAnnotationWithSegment;
import org.ohnlp.familyhistory.tasks.CleanMedTaggerOutput;
import org.ohnlp.familyhistory.tasks.ExtractEligibleEntities;
import org.ohnlp.familyhistory.tasks.SegmentInputSentences;


/**
 * Outputs extracted entities from MedTagger output
 */
public class FHPostprocessTransformEntity extends Transform {

    @Override
    public void initFromConfig(JsonNode jsonNode) throws ComponentInitializationException {
        // Do nothing
    }

    @Override
    public Schema calculateOutputSchema(Schema schema) {
        return ExtractEligibleEntities.ENTITY_SCHEMA;
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

        return associateAnnotationWithSegmentInput.apply(new AssociateAnnotationWithSegment())
                .apply(new ExtractEligibleEntities())
                .get(ExtractEligibleEntities.ALL_ENTITIES_TAG)
                .setCoder(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA));
    }
}
