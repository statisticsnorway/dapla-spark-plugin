package no.ssb.gsim.spark;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

import static no.ssb.gsim.spark.GsimDatasource.CONFIG_LDS_URL;
import static no.ssb.gsim.spark.GsimDatasource.CONFIG_LOCATION_PREFIX;

public class GsimDatasourceTest {

    public static final String UNIT_DATASET_ID = "b9c10b86-5867-4270-b56e-ee7439fe381e";
    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private MockWebServer server;
    private MockResponse unitDatasetResponse;

    @After
    public void tearDown() throws Exception {
        sparkContext.stop();
    }

    @Before
    public void setUp() throws Exception {
        // Create temporary folder and copy test data into it.
        File tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = this.getClass().getResourceAsStream("data/dataset.parquet");
        Path parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);

        // Setup a mock lds server.
        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/lds/");

        // Read the unit dataset json example.
        InputStream in = this.getClass().getResourceAsStream("data/UnitDataSet_Person_1.json");
        String json = new Buffer().readFrom(in).readByteString().utf8();
        json = json.replaceAll("%DATA_PATH%", parquetFile.toString());
        unitDatasetResponse = new MockResponse().setBody(json).setResponseCode(200);

        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config(CONFIG_LDS_URL, baseUrl.toString())
                .config(CONFIG_LOCATION_PREFIX, tempDirectory.toString())
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sqlContext = session.sqlContext();

    }

    @Test
    public void testReadWithId() {
        this.server.enqueue(unitDatasetResponse);
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://" + UNIT_DATASET_ID);
        dataset.show();
    }

    @Test
    public void testWriteWithId() {
        this.server.enqueue(unitDatasetResponse);
        this.server.enqueue(unitDatasetResponse);
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(unitDatasetResponse);

        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://" + UNIT_DATASET_ID);
        dataset.printSchema();
        dataset.show();

        dataset.write().format("no.ssb.gsim.spark").mode(SaveMode.Append).save("lds+gsim://" + UNIT_DATASET_ID);

    }

    @Test
    public void testWriteAndCreateOfLdsObjects() throws InterruptedException {
        this.server.enqueue(unitDatasetResponse);
        this.server.enqueue(unitDatasetResponse);
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(unitDatasetResponse);
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(new MockResponse().setResponseCode(201));
        this.server.enqueue(unitDatasetResponse);

        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://" + UNIT_DATASET_ID);
        dataset.printSchema();
        dataset.show();

        dataset.write()
                .format("no.ssb.gsim.spark")
                .mode(SaveMode.Overwrite)
                .option("create", "dataSetName")
                .save("lds+gsim://create");

        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
        System.out.println(server.takeRequest());
    }
}