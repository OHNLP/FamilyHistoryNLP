package org.ohnlp.familyhistory.subtasks;

import opennlp.tools.cmdline.parser.ParserTool;
import opennlp.tools.parser.Parse;
import opennlp.tools.parser.Parser;
import opennlp.tools.parser.ParserFactory;
import opennlp.tools.parser.ParserModel;
import opennlp.tools.util.Span;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Performs sentence segmentation via constituency parse, and retains second level segments (or top level, if no second level are found)
 */
public class SegmentSentenceViaConstituencyParse extends PTransform<PCollection<Row>, PCollection<Row>> {

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
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply(
                "Map to (document_id, row) tuples",
                ParDo.of(new DoFn<Row, KV<String, Row>>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        Row r = pc.element();
                        pc.output(KV.of(r.getString("document_id"), r));
                    }
                })
        ).setCoder(
                KvCoder.of(StringUtf8Coder.of(), RowCoder.of(DeduplicateAndCleanExtractedSentences.SCHEMA))
        ).apply(
                "Group by Document ID",
                GroupByKey.create()
        ).apply(
                "Generate Segments in Order By Document",
                ParDo.of(new DoFn<KV<String, Iterable<Row>>, Row>() {
                    private transient Parser parser;
                    @StartBundle
                    public void init() throws IOException {
                        InputStream is = SegmentSentenceViaConstituencyParse.class.getResourceAsStream("/org/ohnlp/familyhistory/models/opennlp/en-parser-chunking.bin");
                        ParserModel model = new ParserModel(is);
                        this.parser = ParserFactory.create(model);
                    }

                    @ProcessElement
                    public void process(ProcessContext pc) {
                        String docID = pc.element().getKey();
                        Iterable<Row> sentences = pc.element().getValue();
                        ArrayList<Row> cleanedRows = new ArrayList<>();
                        sentences.forEach(r -> {
                            cleanedRows.add(
                                    Row.withSchema(SCHEMA).addValues(
                                            docID,
                                            r.getInt32("sentence_id"),
                                            -1,
                                            -1,
                                            r.getString("matched_sentence"),
                                            -1,
                                            -1
                                    ).build()
                            );
                        });
                        cleanedRows.sort(Comparator.comparingInt(r -> r.getInt32("sentence_id")));
                        // Now do actual segmentation
                        int documentChunkCount = 0;
                        for (Row r : cleanedRows) {
                            int sentenceChunkCount = 0;
                            String base = r.getString("chunk_text");
                            Parse[] constituencyTree = ParserTool.parseLine(base, this.parser, 1);
                            // Parse out longest non-root S segments
                            List<Span> longestNonRootSegments = getLongestNonRootSegments(constituencyTree[0]);
                            if (longestNonRootSegments.size() == 0) {
                                // Only the top S segment covering whole sentence exists
                                longestNonRootSegments.add(
                                        new Span(0, base.length())
                                );
                            }
                            longestNonRootSegments.sort(Comparator.comparingInt(Span::getStart));
                            int lastEnd = 0;
                            for (int j = 0; j < longestNonRootSegments.size(); j++) {
                                Span span = longestNonRootSegments.get(j);
                                int begin = span.getStart();
                                int end = span.getEnd();
                                // First, check if we need to add a segment in between where last segment ended and the
                                // beginning of this one
                                if (lastEnd != begin) {
                                    pc.output(
                                            Row.withSchema(SCHEMA).addValues(
                                                    r.getString("document_id"),
                                                    r.getInt32("sentence_id"),
                                                    sentenceChunkCount++,
                                                    documentChunkCount++,
                                                    base.substring(lastEnd, begin),
                                                    lastEnd,
                                                    begin
                                            ).build()
                                    );
                                }
                                // Output the regular span
                                pc.output(
                                        Row.withSchema(SCHEMA).addValues(
                                                r.getString("document_id"),
                                                r.getInt32("sentence_id"),
                                                sentenceChunkCount++,
                                                documentChunkCount++,
                                                base.substring(begin, end),
                                                begin,
                                                end
                                        ).build()
                                );
                                // Output trailing span, if necessary
                                if (j == longestNonRootSegments.size() - 1 && end < base.length()) {
                                    pc.output(
                                            Row.withSchema(SCHEMA).addValues(
                                                    r.getString("document_id"),
                                                    r.getInt32("sentence_id"),
                                                    sentenceChunkCount++,
                                                    documentChunkCount++,
                                                    base.substring(end),
                                                    end,
                                                    base.length()
                                            ).build()
                                    );
                                }
                                lastEnd = end;
                            }

                        }
                    }
                })
        );
    }

    private List<Span> getLongestNonRootSegments(Parse constituencyTree) {
        List<Span> ret = new ArrayList<>();
        Parse[] currLevel = constituencyTree.getChildren()[0].getChildren();
        recursSearchFirstSegment(currLevel, ret);
        return ret;
    }

    private void recursSearchFirstSegment(Parse[] currLevel, List<Span> ret) {
        if (currLevel == null || currLevel.length == 0) {
            return;
        }
        for (Parse child : currLevel) {
            if (child.getType().equals("S")) {
                ret.add(child.getSpan());
            } else {
                recursSearchFirstSegment(child.getChildren(), ret);
            }
        }
    }
}
