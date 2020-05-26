package no.ssb.dapla.spark.plugin;
import no.ssb.dapla.dataset.doc.template.SchemaToTemplate;
import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;

public class SparkSchemaConverter {

    public static String toSchemaTemplate(StructType sparkSchema, String path) {
        Schema schema = SchemaConverters.toAvroType(sparkSchema, false, "spark_schema", "");
        SchemaToTemplate generator = new SchemaToTemplate(schema, path);
        return generator.generateSimpleTemplateAsJsonString();
    }

    public static Schema toAvroSchema(StructType sparkSchema, String recordName, String recordNamespace) {
        return SchemaConverters.toAvroType(sparkSchema, false, recordName, recordNamespace);
    }

}
