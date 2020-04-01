package no.ssb.dapla.spark.plugin;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.protobuf.ByteString;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimDatasourceLocalFSTest {

    private static File tempDirectory;
    private static Path parquetFile;
    private MockWebServer dataAccessMockServer;
    private MockWebServer metadataDistributorMockServer;
    private File tempOutPutDirectory;
    private String sparkStoragePath;
    private SparkSession session;

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
        session.stop();
        tempOutPutDirectory.delete();
    }

    @Before
    public void setUp() throws IOException {
        tempOutPutDirectory = Files.createTempDirectory("spark-output").toFile();
        sparkStoragePath = tempOutPutDirectory.toString();
        System.out.println("tempOutPutDirectory created: " + sparkStoragePath);

        this.dataAccessMockServer = new MockWebServer();
        this.dataAccessMockServer.start();
        HttpUrl baseUrl = dataAccessMockServer.url("/data-access/");

        this.metadataDistributorMockServer = new MockWebServer();
        this.metadataDistributorMockServer.start();
        HttpUrl publisherUrl = metadataDistributorMockServer.url("/metadata-publisher/");

        // Read the unit dataset json example.
        session = SparkSession.builder()
                .appName(GsimDatasourceLocalFSTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config("fs.gs.impl.disable.cache", true)
                .config(DaplaSparkConfig.SPARK_SSB_DAPLA_GCS_STORAGE, "file://" + sparkStoragePath)
                .config("spark.ssb.dapla.output.prefix", "test-output")
                .config(DataAccessClient.CONFIG_DATA_ACCESS_URL, baseUrl.toString())
                //.config(DataAccessClient.CONFIG_DATA_ACCESS_URL, "http://localhost:10140/")
                .config(MetadataPublisherClient.CONFIG_METADATA_PUBLISHER_URL, publisherUrl.toString())
                //.config(MetadataPublisherClient.CONFIG_METADATA_PUBLISHER_URL, "http://localhost:10160/")
                .config("spark.ssb.dapla.metadata.writer", NoOpMetadataWriter.class.getName())
                .config("spark.ssb.dapla.metadata.publisher.project.id", "dapla")
                .config("spark.ssb.dapla.metadata.publisher.topic.name", "file-events-1")
                .config("spark.ssb.dapla.oauth.tokenUrl", "http://localhost:28081/auth/realms/ssb/protocol/openid-connect/token")
                .config("spark.ssb.dapla.oauth.clientId", "zeppelin")
                .config("spark.ssb.dapla.oauth.clientSecret", "ed48ee94-fe9d-4096-b069-9626a52877f2")
                .config("spark.ssb.dapla.oauth.ignoreExpiry", "true")
                .config("spark.ssb.username", "kim")
                .config("spark.ssb.access", JWT.create().withClaim("preferred_username", "kim").withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.HOURS))).sign(Algorithm.HMAC256("secret")))
                .config("spark.ssb.refresh", JWT.create().withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.HOURS))).sign(Algorithm.HMAC256("secret")))
                .getOrCreate();

        session.sparkContext().setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla_test-2EYA9GVV2-20200114-173727_1534086404");
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testWrite() throws InterruptedException {
        metadataDistributorMockServer.enqueue(new MockResponse().setResponseCode(200));
        metadataDistributorMockServer.enqueue(new MockResponse().setResponseCode(200));
        long version = System.currentTimeMillis();
        dataAccessMockServer.enqueue(
                new MockResponse().setBody(ProtobufJsonUtils.toString(WriteLocationResponse.newBuilder()
                        .setAccessAllowed(true)
                        .setValidMetadataJson(ByteString.copyFromUtf8("{\n" +
                                "  \"id\": {\n" +
                                "    \"path\": \"/skatt/person/junit\",\n" +
                                "    \"version\": \"" + version + "\"\n" +
                                "  },\n" +
                                "  \"valuation\": \"INTERNAL\",\n" +
                                "  \"state\": \"INPUT\",\n" +
                                "  \"parentUri\": \"file://" + sparkStoragePath + "\",\n" +
                                "  \"createdBy\": \"junit\"\n" +
                                "}"))
                        .setMetadataSignature(ByteString.copyFromUtf8("some-junit-signature"))
                        .build()))
                        .setResponseCode(200)
        );
        dataAccessMockServer.enqueue(
                new MockResponse().setBody(ProtobufJsonUtils.toString(WriteAccessTokenResponse.newBuilder()
                                .setAccessToken("access-token").setExpirationTime(System.currentTimeMillis()).build()))
                .setResponseCode(200)
        );

        Dataset<Row> dataset = session.sqlContext().read()
                .load(parquetFile.toString());

        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                .option("version", version)
                .save("/test/dapla/namespace");

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        RecordedRequest writeLocationRecordedRequest = dataAccessMockServer.takeRequest();
        final WriteLocationRequest writeLocationRequest = ProtobufJsonUtils.toPojo(
                writeLocationRecordedRequest.getBody().readByteString().utf8(), WriteLocationRequest.class);
        assertThat(writeLocationRecordedRequest.getRequestUrl().url().getPath()).isEqualTo("/data-access/rpc/DataAccessService/writeLocation");
        assertThat(writeLocationRequest.getMetadataJson()).isEqualTo("{\n" +
                "  \"id\": {\n" +
                "    \"path\": \"/test/dapla/namespace\",\n" +
                "    \"version\": \"" + version + "\"\n" +
                "  },\n" +
                "  \"valuation\": \"INTERNAL\",\n" +
                "  \"state\": \"INPUT\"\n" +
                "}");

    }

    @Test
    public void testWrite_SparkServiceFail() {
        dataAccessMockServer.enqueue(new MockResponse().setResponseCode(400));
        thrown.expectMessage("En feil har oppstått: Response{protocol=http/1.1, code=400");

        Dataset<Row> dataset = session.sqlContext().read()
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
        dataAccessMockServer.enqueue(new MockResponse().setResponseCode(200));

        thrown.expectMessage("valuation is missing in options");

        Dataset<Row> dataset = session.sqlContext().read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .save("/dapla/namespace");
    }
}