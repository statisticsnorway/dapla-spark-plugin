package no.ssb.dapla.spark.plugin;

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

import static org.assertj.core.api.Assertions.assertThat;

public class GsimDatasourceLocalFSTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private static File tempDirectory;
    private static Path parquetFile;
    private MockWebServer dataAccessMockServer;
    private MockWebServer metadataDistributorMockServer;
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

        this.dataAccessMockServer = new MockWebServer();
        this.dataAccessMockServer.start();
        HttpUrl baseUrl = dataAccessMockServer.url("/data-access/");

        this.metadataDistributorMockServer = new MockWebServer();
        this.metadataDistributorMockServer.start();
        HttpUrl publisherUrl = metadataDistributorMockServer.url("/metadata-publisher/");

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
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
                .config("spark.ssb.access", "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJvN3BfZU5GT0RWdGlzSmZ1MlE1WnBnaVRGd1ZUUlBWRktFZjFWSlFiUEpZIn0.eyJqdGkiOiI2NTAxZDIxMC03YzVhLTQyMzktYmU3Yi1jOTg3MDE3YTBmMzkiLCJleHAiOjE1ODMzOTMwNTMsIm5iZiI6MCwiaWF0IjoxNTgzMzkyNzUzLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjI4MDgxL2F1dGgvcmVhbG1zL3NzYiIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI5NGRjNWI2MS0xNDM0LTRjYjItYWI1NC1mNTU0NmNmNTY2MWUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ6ZXBwZWxpbiIsIm5vbmNlIjoiM050NjBJNkpuTzhTWngwdWhTR2d4OHZncEw0Q1BFSW9kNzFHMzhhaDdoWSIsImF1dGhfdGltZSI6MTU4MzM5Mjc1Mywic2Vzc2lvbl9zdGF0ZSI6ImIzYzkxNjNiLTIxNDUtNGY0NS04OTRmLTY3OTEwOTY0OGU0NiIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cDovL2xvY2FsaG9zdDoyODAxMCJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsIm5hbWUiOiJLaW0iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJraW0iLCJnaXZlbl9uYW1lIjoiS2ltIiwiZW1haWwiOiJkYXBsYS1raW1Ac3NiLm5vIn0.fQZGvvjCsvipcp5MLZTDCu4qseraVxW2PMIwfCpi7aEiCRP3NLsnumKshyrzYrq4lNw0hACGnRd9__aEBG7cgF8QFi2r8me3eG3Q3syxd_UEmxDYwSnSTeune745R2mBIIKWq5fpd9ZYUSnUyEzGhvvyGYWn84LDZphUTrryok-yRRfb-XzhpYTCizYesOtpa2WMwh_b1qtVo1nd3rC2MFR1rl7RvkokNAcB6clRs23AbPPYJvDnXZjmKujbWdzDuWJOnVX0OYzExoe7zUGm693P9EF9JceOI1PhiURiKStVxlYqhNPo8BwR6TxkcJ2BF5I3XeXYd3nvpwFEjzNTLw")
                .config("spark.ssb.refresh", "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5MGJjMWIxNi1lNWIxLTQ3ZmQtYjNmOC1lN2NjMmQ1ZDZmNmYifQ.eyJqdGkiOiJjZGM0NmRjNS1kZTUyLTRhZDQtOGFhYS05ZGQwMzIwMDMxMmQiLCJleHAiOjE1ODMzOTQ1NTMsIm5iZiI6MCwiaWF0IjoxNTgzMzkyNzUzLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjI4MDgxL2F1dGgvcmVhbG1zL3NzYiIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6MjgwODEvYXV0aC9yZWFsbXMvc3NiIiwic3ViIjoiOTRkYzViNjEtMTQzNC00Y2IyLWFiNTQtZjU1NDZjZjU2NjFlIiwidHlwIjoiUmVmcmVzaCIsImF6cCI6InplcHBlbGluIiwibm9uY2UiOiIzTnQ2MEk2Sm5POFNaeDB1aFNHZ3g4dmdwTDRDUEVJb2Q3MUczOGFoN2hZIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiYjNjOTE2M2ItMjE0NS00ZjQ1LTg5NGYtNjc5MTA5NjQ4ZTQ2IiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIn0.MsgxGJCCklagRNfDCgjtxo9rm1HTGGkiOTLpharZ1Ak")
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla_test-2EYA9GVV2-20200114-173727_1534086404");
        this.sqlContext = session.sqlContext();
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

        Dataset<Row> dataset = sqlContext.read()
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
        dataAccessMockServer.enqueue(new MockResponse().setResponseCode(200));

        thrown.expectMessage("valuation is missing in options");

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .save("/dapla/namespace");
    }
}