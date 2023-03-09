package org.ohnlp.familyhistory;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.backbone.api.annotations.ComponentDescription;
import org.ohnlp.backbone.api.annotations.ConfigurationProperty;
import org.ohnlp.backbone.api.components.LoadFromOne;
import org.ohnlp.familyhistory.tasks.CreateFHIRResources;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@ComponentDescription(
        name = "Generate Family History FHIR Resources",
        desc = "Generates FamilyMemberHistory FHIR R4 resources embedded within a DocumentReference and outputs as " +
                "JSON to a specified folder. Note that no patient linkage is performed and as such cross-reference of " +
                "document to patient IDs may be desirable as a post-processing step.",
        requires = {"org.ohnlp.familyhistory.FHPostProcessingTransform"}
)
public class GenerateFHIRFamilyHistoryResources extends LoadFromOne {
    @ConfigurationProperty(
            path = "fileSystemPath",
            desc = "The path into which to write generated FHIR resources"
    )
    private String workingDir;

    public Schema getRequiredColumns(String inputTag) {
        return Schema.of(
                Schema.Field.of("document_id", Schema.FieldType.STRING),
                Schema.Field.of("family_member", Schema.FieldType.STRING),
                Schema.Field.of("side", Schema.FieldType.STRING),
                Schema.Field.of("clinical_entity", Schema.FieldType.STRING),
                Schema.Field.of("concept_codes", Schema.FieldType.STRING),
                Schema.Field.of("certainty", Schema.FieldType.STRING)
        );
    }


    @Override
    public void init() {
        this.workingDir = this.workingDir + File.separator + "DocumentReference";
        if (!new File(this.workingDir).exists()) {
            new File(this.workingDir).mkdirs();
        }
    }

    @Override
    public PDone expand(PCollection<Row> input) {
        input.apply(
                new CreateFHIRResources()
        ).apply("Write FHIR resource JSONs",
                ParDo.of(new DoFn<KV<String, String>, Void>() {
                    @ProcessElement
                    public void process(ProcessContext pc) {
                        String fileID = pc.element().getKey();
                        String out = pc.element().getValue();
                        try (FileWriter fw = new FileWriter(new File(workingDir, fileID))) {
                            fw.write(out);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
        );
        return PDone.in(input.getPipeline());
    }
}
