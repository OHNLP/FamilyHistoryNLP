package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.familyhistory.subtasks.DeduplicateAndCleanExtractedSentences;
import org.ohnlp.familyhistory.subtasks.SegmentSentenceViaConstituencyParse;

/**
 * Segments sentences via constituency parse, using longest S spans present expecting root (which is the entire sentence)
 * If no other S spans present, returns whole sentence
 */
public class SegmentInputSentences extends PTransform<PCollection<Row>, PCollection<Row>> {

    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("chunk_id", Schema.FieldType.INT32),
            Schema.Field.of("sequenced_chunk_id_in_document", Schema.FieldType.INT32),
            Schema.Field.of("chunk_text", Schema.FieldType.STRING),
            Schema.Field.of("chunk_start_offset", Schema.FieldType.INT32),
            Schema.Field.of("chunk_end_offset", Schema.FieldType.INT32)
    );

    @Override
    public PCollection<Row> expand(PCollection<Row> df) {
        // First, to reduce duplicate constituency parsing work, get unique sentences only (since input has one sentence per annotation
        df = df.apply("Get distinct sentences only", new DeduplicateAndCleanExtractedSentences());
        // Now re-map to segments
        return df.apply("Segment Sentences", new SegmentSentenceViaConstituencyParse()).setRowSchema(SCHEMA);
    }
}
