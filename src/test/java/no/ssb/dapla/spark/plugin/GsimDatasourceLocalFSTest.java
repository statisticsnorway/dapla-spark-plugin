package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.NoOpMetadataWriter;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.ssb.dapla.spark.plugin.metadata.LocalFSMetaDataWriter.DATASET_META_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

public class GsimDatasourceLocalFSTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private static File tempDirectory;
    private static Path parquetFile;
    private MockWebServer server;
    private MockWebServer publisher;
    private File tempOutPutDirectory;
    private String sparkStoragePath;

    @BeforeClass
    public static void setupTestData() throws Exception {
        // Create temporary folder and copy test data into it.
        tempDirectory = Files.createTempDirectory("dapla-gsim-spark").toFile();
        InputStream parquetContent = GsimDatasourceLocalFSTest.class.getResourceAsStream("data/dataset.parquet");
        parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);
        System.out.println("File created: " + parquetFile.toString());
    }

    @AfterClass
    public static void removeTestData() {
        parquetFile.toFile().delete();
        tempDirectory.delete();
    }

    @After
    public void tearDown() {
        sparkContext.stop();
        tempOutPutDirectory.delete();
    }

    @Before
    public void setUp() throws IOException {
        tempOutPutDirectory = Files.createTempDirectory("spark-output").toFile();
        sparkStoragePath = tempOutPutDirectory.toString();
        System.out.println("tempOutPutDirectory created: " + sparkStoragePath);

        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/spark-service/");

        this.publisher = new MockWebServer();
        this.publisher.start();
        HttpUrl publisherUrl = publisher.url("/metadata-publisher/");

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceLocalFSTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config("fs.gs.impl.disable.cache", true)
                .config(DaplaSparkConfig.SPARK_SSB_DAPLA_GCS_STORAGE, "file://" + sparkStoragePath)
                .config("spark.ssb.dapla.output.prefix", "test-output")
                .config(DataAccessClient.CONFIG_DATA_ACCESS_URL, baseUrl.toString())
                .config(MetadataPublisherClient.CONFIG_METADATA_PUBLISHER_URL, publisherUrl.toString())
                .config("spark.ssb.dapla.metadata.writer", NoOpMetadataWriter.class.getName())
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla_test-2EYA9GVV2-20200114-173727_1534086404");
        this.sqlContext = session.sqlContext();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testWrite() throws InterruptedException, IOException {
        LocationResponse locationResponse = createLocationResponse();
        server.enqueue(new MockResponse().setBody(ProtobufJsonUtils.toString(locationResponse)).setResponseCode(200));
        publisher.enqueue(new MockResponse().setResponseCode(200));

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                .save("/rawdata/skatt/konto");
        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        RecordedRequest recordedRequest = server.takeRequest();

        assertThat(recordedRequest.getRequestUrl().url().getPath()).isEqualTo("/spark-service/rpc/DataAccessService/getLocation");
        String json = recordedRequest.getBody().readByteString().utf8();
        System.out.println(json);
        LocationRequest request = ProtobufJsonUtils.toPojo(json, LocationRequest.class);
        assertThat(request.getPath()).isEqualTo("/rawdata/skatt/konto");
        assertThat(request.getUserId()).isEqualTo("dapla_test");

        // This does not longer work. Fix later
        /*
        String metaDatafileName = sparkStoragePath + "/" + DATASET_META_FILE_NAME;
        try (Stream<String> stream = Files.lines(Paths.get(metaDatafileName))) {
            String all = stream.collect(Collectors.joining("\n"));
            DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(all, DatasetMeta.class);

            assertThat(datasetMeta.getParentUri()).isEqualTo(sparkStoragePath);
            assertThat(datasetMeta.getValuation().name()).isEqualTo("INTERNAL");
            assertThat(datasetMeta.getState().name()).isEqualTo("INPUT");

            assertThat(datasetMeta.getId().getPath()).isEqualTo("/rawdata/skatt/konto");
            assertThat(datasetMeta.getId().getVersion()).isLessThan(System.currentTimeMillis() + 1);
        }
         */
    }

    @Test
    public void testWrite_SparkServiceFail() {
        server.enqueue(new MockResponse().setResponseCode(400));
        thrown.expectMessage("En feil har oppstått: Response{protocol=http/1.1, code=400");

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                .save("/dapla/namespace");
    }

    @Test
    public void write_Missing_Valuation() {
        LocationResponse locationResponse = createLocationResponse();
        server.enqueue(new MockResponse().setBody(ProtobufJsonUtils.toString(locationResponse)).setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));

        thrown.expectMessage("valuation missing from parametersMap(path -> /dapla/namespace)");

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .save("/dapla/namespace");
    }

    private LocationResponse createLocationResponse() {
        return LocationResponse.newBuilder()
                .setParentUri("file://" + sparkStoragePath)
                .setVersion("1")
                .build();
    }
}