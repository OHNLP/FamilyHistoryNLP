package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;

public class GenerateCandidatePairsSameSentenceChunk extends PTransform<PCollectionTuple, PCollection<Row>> {
    @Override
    public PCollection<Row> expand(PCollectionTuple input) {
        return null;
    }
}
