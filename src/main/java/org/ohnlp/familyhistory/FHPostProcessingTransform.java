package org.ohnlp.familyhistory;

import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.components.OneToManyTransform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.familyhistory.tasks.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@ComponentDescription(
        name = "Family History Postprocessing",
        desc = "Extracts and Creates Family History Relations",
        requires = {"org.ohnlp.medtagger.backbone.MedTaggerBackboneTransform"}
)
public class FHPostProcessingTransform extends OneToManyTransform {

    public Schema getRequiredColumns(String inputTag) {
        return Schema.of(
                Schema.Field.of("note_id", Schema.FieldType.STRING),
                Schema.Field.of("sentid", Schema.FieldType.STRING),
                Schema.Field.of("concept_code", Schema.FieldType.STRING),
                Schema.Field.of("matched_sentence", Schema.FieldType.STRING),
                Schema.Field.of("certainty", Schema.FieldType.STRING),
                Schema.Field.of("offset", Schema.FieldType.INT32),
                Schema.Field.of("sent_offset", Schema.FieldType.INT32),
                Schema.Field.of("semgroups", Schema.FieldType.STRING)
        );
    }


    @Override
    public Map<String, Schema> calculateOutputSchema(Schema schema) {
        return Map.of(
                "Segments", SegmentInputSentences.SCHEMA,
                "Entities", ExtractEligibleEntities.ENTITY_SCHEMA,
                "Relations", GenerateRelationPairs.SCHEMA
        );
    }

    @Override
    public PCollectionRowTuple expand(PCollection<Row> input) {
        // First, get Segments
        PCollection<Row> cleanedInput = input.apply(
                new CleanMedTaggerOutput()
        ).setRowSchema(CleanMedTaggerOutput.SCHEMA);
        PCollection<Row> segments = cleanedInput.apply(
                new SegmentInputSentences()
        ).setRowSchema(SegmentInputSentences.SCHEMA);
        // Now get entities
        PCollectionRowTuple associateAnnotationWithSegmentInput = PCollectionRowTuple.of(
                AssociateAnnotationWithSegment.SEGMENT_META_TAG,
                segments
        ).and(
                AssociateAnnotationWithSegment.ANNOTATIONS_TAG,
                cleanedInput
        );

        PCollectionTuple extractedEntityTuple = associateAnnotationWithSegmentInput.apply(
                new AssociateAnnotationWithSegment()
        ).apply(
                new ExtractEligibleEntities()
        );
        PCollection<Row> entities = extractedEntityTuple.get(
                ExtractEligibleEntities.ALL_ENTITIES_TAG
        ).setCoder(RowCoder.of(ExtractEligibleEntities.ENTITY_SCHEMA));

        PCollection<Row> relnPairs = extractedEntityTuple.apply(new GenerateRelationPairs());
        return PCollectionRowTuple.of(
                "Segments", segments
        ).and(
                "Entities", entities
        ).and(
                "Relations", relnPairs
        );
    }

    @Override
    public void init() throws ComponentInitializationException {

    }

    @Override
    public List<String> getOutputTags() {
        return Arrays.asList("Segments", "Entities", "Relations");
    }

    @Override
    public String getInputTag() {
        return "MedTagger Transform Output";
    }
}

