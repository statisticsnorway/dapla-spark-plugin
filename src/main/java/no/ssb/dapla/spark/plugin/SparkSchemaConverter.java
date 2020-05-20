package no.ssb.dapla.spark.plugin;
import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;

public class SparkSchemaConverter {

    public static Schema toAvroSchema(StructType sparkSchema, String recordName, String recordNamespace) {
        return SchemaConverters.toAvroType(sparkSchema, false, recordName, recordNamespace);
    }
}
