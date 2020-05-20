package no.ssb.dapla.spark.plugin;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.*;

public class SparkSchemaConverterTest {

    private SparkSession session;
    private static final String simpleSchema = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"name\" : \"spark_schema\",\n" +
            "  \"namespace\" : \"namespace\",\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"PERSON_ID\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"INCOME\",\n" +
            "    \"type\" : [ \"long\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"GENDER\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"MARITAL_STATUS\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"MUNICIPALITY\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  }, {\n" +
            "    \"name\" : \"DATA_QUALITY\",\n" +
            "    \"type\" : [ \"string\", \"null\" ]\n" +
            "  } ]\n" +
            "}";

    @After
    public void tearDown() {
        session.stop();
    }

    @Before
    public void setUp() {
        session = SparkSession.builder()
                .appName(GsimDatasourceLocalFSTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false).getOrCreate();

    }
    @Test
    public void generateAvroSchema() {
        File parquetFile = new File(GsimDatasourceLocalFSTest.class.getResource("data/dataset.parquet").getFile());
        Dataset<Row> dataset = session.sqlContext().read()
                .load(parquetFile.toString());
        Schema schema = SparkSchemaConverter.toAvroSchema(dataset.schema(), "spark_schema", "namespace");
        //System.out.println(schema.toString(true));
        assertThat(schema.toString(true)).isEqualTo(simpleSchema);
    }
}