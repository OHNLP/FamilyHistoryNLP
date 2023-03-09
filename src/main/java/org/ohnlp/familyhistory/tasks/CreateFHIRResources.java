package org.ohnlp.familyhistory.tasks;

import ca.uhn.fhir.context.FhirContext;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.*;

import java.util.*;

public class CreateFHIRResources extends PTransform<PCollection<Row>, PCollection<KV<String, String>>> {

    private enum FamMemberCode {
        FATHER("FTH", "father"),
        MOTHER("MTH", "mother"),
        PARENT("PRN", "parent"),
        SISTER("SIS", "sister"),
        BROTHER("BRO", "brother"),
        DAUGHTER("DAUC", "daughter"),
        SON("SONC", "son"),
        CHILD("CHILD", "child"),
        SIBLING("SIB", "sibling"),
        GRANDMOTHER("GRMTH", "grandmother"),
        GRANDFATHER("GRFTH", "grandfather"),
        GRANDCHILD("GRNDCHILD", "grandchild"),
        GRANDDAUGHTER("GRNDDAU", "granddaughter"),
        GRANDSON("GRNDSON", "grandson"),
        GRANDPARENT("GRPRN", "grandparent"),
        COUSIN("COUSN", "cousin"),
        AUNT("AUNT", "aunt"),
        UNCLE("UNCLE", "uncle");

        private final String code;
        private final String display;

        FamMemberCode(String code, String display) {
            this.code = code;
            this.display = display;
        }
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<Row> input) {
        return input.apply("Group by document", ParDo.of(new DoFn<Row, KV<String, Row>>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                Row r = pc.element();
                String docId = r.getString("document_id");
                pc.output(KV.of(docId, r));
            }
        })).setCoder(KvCoder.of(StringUtf8Coder.of(), input.getCoder())).apply(
                GroupByKey.create()
        ).apply("Map to FHIR Resources", ParDo.of(new DoFn<KV<String, Iterable<Row>>, KV<String, String>>() {

            private FhirContext context;

            @StartBundle
            public void init() {
                this.context = FhirContext.forR4Cached();
            }

            @ProcessElement
            public void process(ProcessContext pc) {
                KV<String, Iterable<Row>> element = pc.element();
                // First, set up global document level object
                DocumentReference docRef = new DocumentReference();
                docRef.setMasterIdentifier(new Identifier().setValue("DocumentReference/" + element.getKey()));
                // Group by family member concept
                Map<KV<String, String>, List<Row>> rowsByFamilyMember = new HashMap<>();
                for (Row r : element.getValue()) {
                    String fm = r.getString("family_member").toUpperCase(Locale.ROOT);
                    // First, calculate number of greats
                    int numGreats = 0;
                    int numGrand = 0;
                    while (fm.contains("GREAT_")) {
                        numGreats++;
                        fm = fm.replaceFirst("GREAT_", "");
                    }
                    while (fm.contains("GRAND_")) {
                        numGrand++;
                        fm = fm.replaceFirst("GRAND_", "");
                    }
                    FamMemberCode famMember = FamMemberCode.valueOf(fm);
                    String side = r.getString("side").toUpperCase(Locale.ROOT);
                    String display = famMember.display;
                    String code = famMember.code;
                    // Determine greats/grand
                    for (int i = 0; i < numGreats; i++) {
                        display = "great-" + display;
                        code = "G" + code;
                    }
                    // Determine paternal/maternal
                    if (side.equalsIgnoreCase("paternal")) {
                        display = "paternal " + display;
                        code = "P" + code;
                    } else if (side.equalsIgnoreCase("maternal")) {
                        display = "maternal " + display;
                        code = "M" + code;
                    }
                    if (numGrand > 0) {
                        display = "extended family member";
                        code = "EXT";
                    }
                    rowsByFamilyMember.computeIfAbsent(KV.of(code, display), k -> new ArrayList<>()).add(r);
                }
                // Iterate by family member and append respective family member history
                rowsByFamilyMember.forEach((famMember, entries) -> {
                    FamilyMemberHistory fmHistory = new FamilyMemberHistory();
                    fmHistory.setRelationship(new CodeableConcept().addCoding(new Coding().setCode(famMember.getKey()).setDisplay(famMember.getValue()).setSystem("https://terminology.hl7.org/CodeSystem/v3-RoleCode")));
                    for (Row r : entries) {
                        String display = r.getString("clinical_entity");
                        String[] conceptCodes = r.getString("concept_codes").split(";");
                        String certaintyRaw = r.getString("certainty");
                        CodeableConcept conditionConcept = new CodeableConcept();
                        for (String code : conceptCodes) {
                            conditionConcept.addCoding().setCode(code).setSystem("https://snomed.info/sct").setDisplay(display);
                        }
                        fmHistory.addCondition().setCode(conditionConcept).addExtension().setValue(new StringType(certaintyRaw)).setId("assertion");
                    }
                    docRef.addContained(fmHistory);
                });
                pc.output(KV.of(element.getKey() + ".json", this.context.newJsonParser().setPrettyPrint(true).encodeResourceToString(docRef)));
            }
        })).setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    }
}
