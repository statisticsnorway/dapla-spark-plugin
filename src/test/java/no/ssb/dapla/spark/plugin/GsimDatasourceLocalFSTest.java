package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimDatasourceLocalFSTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private static File tempDirectory;
    private static Path parquetFile;
    private MockWebServer server;
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
        System.out.println("tempOutPutDirectory created: " + tempOutPutDirectory.toString());

        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/spark-service/");

        // Read the unit dataset json example.
        sparkStoragePath = "file://" + tempOutPutDirectory.toString();
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceLocalFSTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config("fs.gs.impl.disable.cache", true)
                .config("spark.ssb.dapla.gcs.storage", sparkStoragePath)
                .config("spark.ssb.dapla.output.prefix", "test-output")
                .config("spark.ssb.dapla.router.url", baseUrl.toString())
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla-test-2EYA9GVV2-20200114-173727_1534086404");
        this.sqlContext = session.sqlContext();
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testWrite() throws InterruptedException {
        no.ssb.dapla.catalog.protobuf.Dataset datasetMock = createMockDataset("");
        server.enqueue(new MockResponse().setBody(ProtobufJsonUtils.toString(datasetMock)).setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(201));

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                .save("dapla.namespace");
        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        assertThat(server.takeRequest().getRequestUrl().query()).isEqualTo(
                "name=dapla.namespace&operation=CREATE&valuation=INTERNAL&state=INPUT&userId=dapla-test");

        String json = server.takeRequest().getBody().readByteString().utf8();
        no.ssb.dapla.catalog.protobuf.Dataset dataSet = ProtobufJsonUtils.toPojo(json, no.ssb.dapla.catalog.protobuf.Dataset.class);
        String location = dataSet.getLocations(0);

        assertThat(location).containsPattern(sparkStoragePath + "/test-output/mockId/\\d+");
    }

    @Test
    public void testWrite_SparkServiceFail()  {
        no.ssb.dapla.catalog.protobuf.Dataset datasetMock = createMockDataset("");
        server.enqueue(new MockResponse().setBody(ProtobufJsonUtils.toString(datasetMock)).setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(500));

        thrown.expectMessage("En feil har oppstått: Response{protocol=http/1.1, code=500");

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                .save("dapla.namespace");
    }

    @Test
    public void write_Missing_Valuation() throws InterruptedException {
        no.ssb.dapla.catalog.protobuf.Dataset datasetMock = createMockDataset("");
        server.enqueue(new MockResponse().setBody(ProtobufJsonUtils.toString(datasetMock)).setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));

        thrown.expectMessage("valuation missing from parametersMap(path -> dapla.namespace)");

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .save("dapla.namespace");
    }


    private no.ssb.dapla.catalog.protobuf.Dataset createMockDataset(String location) {
        return no.ssb.dapla.catalog.protobuf.Dataset.newBuilder()
                .setId(DatasetId.newBuilder().setId("mockId").addName("dapla.namespace").build())
                .setValuation(no.ssb.dapla.catalog.protobuf.Dataset.Valuation.valueOf("SENSITIVE"))
                .setState(no.ssb.dapla.catalog.protobuf.Dataset.DatasetState.valueOf("INPUT"))
                .addLocations(location).build();
    }


}