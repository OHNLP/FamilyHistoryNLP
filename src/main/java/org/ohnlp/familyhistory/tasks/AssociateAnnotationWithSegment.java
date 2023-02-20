package org.ohnlp.familyhistory.tasks;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

public class AssociateAnnotationWithSegment extends PTransform<PCollectionRowTuple, PCollection<Row>> {
    public static final String SEGMENT_META_TAG = "SEGMENT_METADATA";
    public static final String ANNOTATIONS_TAG = "ANNOTATIONS";

    public static Schema SCHEMA = Schema.of(
            Schema.Field.of("document_id", Schema.FieldType.STRING),
            Schema.Field.of("sentence_id", Schema.FieldType.INT32),
            Schema.Field.of("chunk_id", Schema.FieldType.INT32),
            Schema.Field.of("sequenced_chunk_id_in_document", Schema.FieldType.INT32),
            Schema.Field.of("chunk_text", Schema.FieldType.STRING),
            Schema.Field.of("matched_text", Schema.FieldType.STRING),
            Schema.Field.of("concept_code", Schema.FieldType.STRING),
            Schema.Field.of("offset", Schema.FieldType.INT32),
            Schema.Field.of("certainty", Schema.FieldType.STRING),
            Schema.Field.of("semgroups", Schema.FieldType.STRING).withNullable(true)
    );


    @Override
    public PCollection<Row> expand(PCollectionRowTuple input) {
        PCollection<Row> segments = input.get(SEGMENT_META_TAG);
        PCollection<Row> entities = input.get(ANNOTATIONS_TAG);
        return entities.apply("Join by document and sentence",
                Join.<Row, Row>innerJoin(segments).using("document_id", "sentence_id")
        ).apply(
                "Filter to only offsets falling within segment offsets",
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row e = pc.element();
                        Row entity = e.getRow("lhs");
                        Row segmentMeta = e.getRow("rhs");
                        int offset_within_sentence = entity.getInt32("offset") - entity.getInt32("sent_offset");
                        if (offset_within_sentence >= segmentMeta.getInt32("chunk_start_offset") && offset_within_sentence < segmentMeta.getInt32("chunk_end_offset")) {
                            pc.output(Row.withSchema(SCHEMA).addValues(
                                    segmentMeta.getString("document_id"),
                                    segmentMeta.getInt32("sentence_id"),
                                    segmentMeta.getInt32("chunk_id"),
                                    segmentMeta.getInt32("sequenced_chunk_id_in_document"),
                                    segmentMeta.getString("chunk_text"),
                                    entity.getString("matched_text"),
                                    entity.getString("concept_code"),
                                    entity.getInt32("offset"),
                                    entity.getString("certainty"),
                                    entity.getString("semgroups")
                            ).build());
                        }
                    }
                })
        ).setRowSchema(SCHEMA);
    }
}
