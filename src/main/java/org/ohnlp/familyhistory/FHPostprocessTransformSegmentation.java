package org.ohnlp.familyhistory;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.Transform;
import org.ohnlp.backbone.api.exceptions.ComponentInitializationException;
import org.ohnlp.familyhistory.subtasks.SegmentSentenceViaConstituencyParse;
import org.ohnlp.familyhistory.tasks.CleanMedTaggerOutput;
import org.ohnlp.familyhistory.tasks.SegmentInputSentences;

/**
 * Outputs segmented sentence chunks and associated IDs. Only sentences containing an NLP output will be displayed
 */
public class FHPostprocessTransformSegmentation extends Transform {
    @Override
    public void initFromConfig(JsonNode jsonNode) throws ComponentInitializationException {

    }

    @Override
    public Schema calculateOutputSchema(Schema schema) {
        return SegmentInputSentences.SCHEMA;
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply(new CleanMedTaggerOutput()).apply(new SegmentInputSentences());
    }
}
