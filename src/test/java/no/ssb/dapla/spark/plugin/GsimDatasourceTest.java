package no.ssb.dapla.spark.plugin;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimDatasourceTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private File tempDirectory;
    private Path parquetFile;

    @After
    public void tearDown() {
        sparkContext.stop();
    }

    @Before
    public void setUp() throws Exception {
        // Create temporary folder and copy test data into it.
        tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = this.getClass().getResourceAsStream("data/dataset.parquet");
        parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sqlContext = session.sqlContext();

    }

    @Test
    public void testReadWithId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.dapla.spark.plugin")
                .load(parquetFile.toString());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();
    }

    @Test
    @Ignore
    public void testReadWithIdAndCallServer() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.dapla.spark.plugin")
                .option("uri_to_dapla_plugin_server", "localhost:8092")
                .load(parquetFile.toString());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();
    }

    @Test
    public void testReadWrite() {
        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
                .load(parquetFile.toString());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        System.out.println(tempDirectory);
        dataset.write().format("no.ssb.dapla.spark.plugin").mode(SaveMode.Overwrite).save(tempDirectory + "/out.parquet");
    }
}