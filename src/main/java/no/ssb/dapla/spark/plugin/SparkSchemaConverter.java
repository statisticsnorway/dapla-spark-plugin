package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.dataset.doc.builder.LineageBuilder;
import no.ssb.dapla.dataset.doc.template.SchemaToTemplate;
import no.ssb.dapla.dataset.doc.traverse.SchemaWithPath;
import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class SparkSchemaConverter {

    public static String toSchemaTemplate(StructType sparkSchema, String simple) {
        Schema schema = SchemaConverters.toAvroType(sparkSchema, false, "spark_schema", "");
        SchemaToTemplate generator = new SchemaToTemplate(schema);
        return generator.withDoSimpleFiltering(Boolean.parseBoolean(simple)).generateSimpleTemplateAsJsonString();
    }

    public static String toLineageTemplate(StructType sparkOutputSchema, List<Map<String, StructType>> mapList) {
        Schema outputSchema = SchemaConverters.toAvroType(sparkOutputSchema, false, "spark_schema", "");
        LineageBuilder.SchemaToLineageBuilder lineageBuilder =
                LineageBuilder.createSchemaToLineageBuilder()
                        .outputSchema(outputSchema);

        for (Map<String, StructType> item : mapList) {
            for (Map.Entry<String, StructType> entry : item.entrySet()) {
                Schema inputSchema = SchemaConverters.toAvroType(entry.getValue(), false, "spark_schema", "");
                lineageBuilder.addInput(new SchemaWithPath(inputSchema, entry.getKey(), 123));
            }
        }
        return lineageBuilder.build().generateTemplateAsJsonString();
    }


    public static Schema toAvroSchema(StructType sparkSchema, String recordName, String recordNamespace) {
        return SchemaConverters.toAvroType(sparkSchema, false, recordName, recordNamespace);
    }

}
