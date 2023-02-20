package org.ohnlp.familyhistory.subtasks;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.familyhistory.tasks.ExtractEligibleEntities;

/**
 * Marks up the input sentences into the annotated_sentence field using extracted entities.
 * Markup takes the format "matched_text"-"semgroup":"norm_form" (including quotations)
 */
public class AnnotateEntitiesInSentences extends PTransform<PCollection<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        return input.apply("Map to <(file, sentence_id, chunk_id_in_sentence, chunk_id_in_document, sentence), (matched_text, semtype, norm)> KV tuple pairs",
                        ParDo.of(new DoFn<Row, KV<KV<String, KV<Integer, KV<Integer, KV<Integer, String>>>>, KV<KV<String, String>, String>>>() {
                            @ProcessElement
                            public void process(ProcessContext pc) {
                                Row r = pc.element();
                                pc.output(
                                        KV.of(
                                                KV.of(r.getString("document_id"),
                                                        KV.of(r.getInt32("sentence_id"),
                                                                KV.of(r.getInt32("chunk_id"),
                                                                        KV.of(
                                                                                r.getInt32("sequenced_chunk_id_in_document"), r.getString("chunk_text")
                                                                        )
                                                                )
                                                        )
                                                ),
                                                KV.of(
                                                        KV.of( // Use KV here as Java native doesn't have tuples
                                                                r.getString("matched_text"),
                                                                r.getString("semgroups") == null ? "null" : r.getString("semgroups")),
                                                        r.getString("concept_code"))
                                        )
                                );
                            }
                        }))
                .apply("Group by (file, sentence_id, chunk_id, sentence) and aggregate (matched_text, semtype, norm))", GroupByKey.create())
                .apply("Replace all entity mentions in sentence with annotated versions",
                        ParDo.of(new DoFn<KV<KV<String, KV<Integer, KV<Integer, KV<Integer, String>>>>, Iterable<KV<KV<String, String>, String>>>, Row>() {
                            @ProcessElement
                            public void process(ProcessContext pc) {
                                KV<KV<String, KV<Integer, KV<Integer, KV<Integer, String>>>>, Iterable<KV<KV<String, String>, String>>> element = pc.element();
                                String docID = element.getKey().getKey();
                                int chunkIdxInDocument = element.getKey().getValue().getValue().getValue().getKey();
                                String sentence = sanitize(element.getKey().getValue().getValue().getValue().getValue());

                                for (KV<KV<String, String>, String> e : element.getValue()) {
                                    String matchedText = e.getKey().getKey();
                                    String semgroup = e.getKey().getValue();
                                    if (semgroup == null) {
                                        semgroup = "null";
                                    }
                                    String norm = e.getValue();
                                    String replacement = "\"" + sanitize(matchedText) + "\"-\"" + sanitize(semgroup) + "\":\"" + sanitize(norm) + "\"";
                                    // Replace all mentions of "matched_text" with ""matched_text"-"semgroups":"norm"
                                    sentence = sentence.replaceAll(sanitize(matchedText), replacement);
                                }
                                pc.output(Row.withSchema(ExtractEligibleEntities.ANNOTATED_SENTENCE_SCHEMA).addValues(docID,
                                        element.getKey().getValue().getKey(),
                                        element.getKey().getValue().getValue().getKey(),
                                        chunkIdxInDocument,
                                        element.getKey().getValue().getValue().getValue().getValue(), sentence).build());
                            }
                        }));
    }

    private String sanitize(String in) { // TODO check if questionmark should have whitespace replacement
        return in.replaceAll("[\"()'\\[.;,<>]", "").replaceAll("[/\\]+&?]", " ");
    }
}
