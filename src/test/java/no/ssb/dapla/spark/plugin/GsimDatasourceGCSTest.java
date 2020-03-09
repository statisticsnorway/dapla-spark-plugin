package no.ssb.dapla.spark.plugin;

import com.google.api.gax.paging.Page;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.gcs.connector.GoogleHadoopFileSystemExt;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.FilesystemMetaDataWriter;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimDatasourceGCSTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private static File tempDirectory;
    private static Path parquetFile;
    private static String bucket;
    private static String namespace = "test/dapla/namespace";
    private static long version = System.currentTimeMillis();
    private static BlobId blobId;
    private MockWebServer server;
    private MockWebServer publisher;

    @BeforeClass
    public static void setupBucketFolder() throws Exception {
        // Verify the test environment
        if (System.getenv().get(GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE) == null) {
            throw new IllegalStateException(String.format("Missing environment variable: " +
                    GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE));
        }
        // Setup GCS test bucket
        bucket = Optional.ofNullable(System.getenv().get("DAPLA_SPARK_TEST_BUCKET")).orElse("dev-datalager-store");
        // Create temporary folder and copy test data into it.
        tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = GsimDatasourceGCSTest.class.getResourceAsStream("data/dataset.parquet");
        parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);
        System.out.println("File created: " + parquetFile.toString());
        blobId = createBucketTestFile(Files.readAllBytes(parquetFile));
    }

    @AfterClass
    public static void clearBucketFolder() {
        final Storage storage = getStorage();
        Page<Blob> page = storage.list(bucket, Storage.BlobListOption.prefix(namespace + "/"));
        BlobId[] blobs = StreamSupport.stream(page.iterateAll().spliterator(), false).map(BlobInfo::getBlobId).collect(Collectors.toList()).toArray(new BlobId[0]);
        if (blobs.length > 0) {
            List<Boolean> deletedList = storage.delete(blobs);
            for (Boolean deleted : deletedList) {
                if (!deleted) {
                    throw new RuntimeException("Unable to delete blob in bucket");
                }
            }
        }
        parquetFile.toFile().delete();
        tempDirectory.delete();
    }

    @After
    public void tearDown() {
        sparkContext.stop();
    }

    @Before
    public void setUp() throws IOException {
        // Mock user read by org.apache.hadoop.security.UserGroupInformation
        System.setProperty("HADOOP_USER_NAME", "dapla_test");

        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/spark-service-gcs/");

        this.publisher = new MockWebServer();
        this.publisher.start();
        HttpUrl publisherUrl = publisher.url("/metadata-publisher/");

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceGCSTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, true)
                .config(DaplaSparkConfig.SPARK_SSB_DAPLA_GCS_STORAGE, "gs://" + bucket)
                .config(DataAccessClient.CONFIG_DATA_ACCESS_URL, baseUrl.toString())
                //.config(DataAccessClient.CONFIG_DATA_ACCESS_URL, "http://localhost:10140/")
                .config(MetadataPublisherClient.CONFIG_METADATA_PUBLISHER_URL, publisherUrl.toString())
                //.config(MetadataPublisherClient.CONFIG_METADATA_PUBLISHER_URL, "http://localhost:10160/")
                .config("spark.ssb.dapla.metadata.writer", FilesystemMetaDataWriter.class.getCanonicalName())
                .config("spark.hadoop.fs.gs.impl", GoogleHadoopFileSystemExt.class.getCanonicalName())
                .config("spark.hadoop.fs.gs.delegation.token.binding", BrokerDelegationTokenBinding.class.getCanonicalName())
                //.config("spark.hadoop.fs.gs.auth.access.token.provider.impl", BrokerAccessTokenProvider.class.getCanonicalName())
                .config("spark.ssb.dapla.metadata.publisher.project.id", "dapla")
                .config("spark.ssb.dapla.metadata.publisher.topic.name", "file-events-1")
                .config("spark.ssb.username", "kim")
                .config("spark.ssb.access", "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJvN3BfZU5GT0RWdGlzSmZ1MlE1WnBnaVRGd1ZUUlBWRktFZjFWSlFiUEpZIn0.eyJqdGkiOiI2NTAxZDIxMC03YzVhLTQyMzktYmU3Yi1jOTg3MDE3YTBmMzkiLCJleHAiOjE1ODMzOTMwNTMsIm5iZiI6MCwiaWF0IjoxNTgzMzkyNzUzLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjI4MDgxL2F1dGgvcmVhbG1zL3NzYiIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI5NGRjNWI2MS0xNDM0LTRjYjItYWI1NC1mNTU0NmNmNTY2MWUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ6ZXBwZWxpbiIsIm5vbmNlIjoiM050NjBJNkpuTzhTWngwdWhTR2d4OHZncEw0Q1BFSW9kNzFHMzhhaDdoWSIsImF1dGhfdGltZSI6MTU4MzM5Mjc1Mywic2Vzc2lvbl9zdGF0ZSI6ImIzYzkxNjNiLTIxNDUtNGY0NS04OTRmLTY3OTEwOTY0OGU0NiIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cDovL2xvY2FsaG9zdDoyODAxMCJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsIm5hbWUiOiJLaW0iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJraW0iLCJnaXZlbl9uYW1lIjoiS2ltIiwiZW1haWwiOiJkYXBsYS1raW1Ac3NiLm5vIn0.fQZGvvjCsvipcp5MLZTDCu4qseraVxW2PMIwfCpi7aEiCRP3NLsnumKshyrzYrq4lNw0hACGnRd9__aEBG7cgF8QFi2r8me3eG3Q3syxd_UEmxDYwSnSTeune745R2mBIIKWq5fpd9ZYUSnUyEzGhvvyGYWn84LDZphUTrryok-yRRfb-XzhpYTCizYesOtpa2WMwh_b1qtVo1nd3rC2MFR1rl7RvkokNAcB6clRs23AbPPYJvDnXZjmKujbWdzDuWJOnVX0OYzExoe7zUGm693P9EF9JceOI1PhiURiKStVxlYqhNPo8BwR6TxkcJ2BF5I3XeXYd3nvpwFEjzNTLw")
                .config("spark.ssb.refresh", "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5MGJjMWIxNi1lNWIxLTQ3ZmQtYjNmOC1lN2NjMmQ1ZDZmNmYifQ.eyJqdGkiOiJjZGM0NmRjNS1kZTUyLTRhZDQtOGFhYS05ZGQwMzIwMDMxMmQiLCJleHAiOjE1ODMzOTQ1NTMsIm5iZiI6MCwiaWF0IjoxNTgzMzkyNzUzLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjI4MDgxL2F1dGgvcmVhbG1zL3NzYiIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6MjgwODEvYXV0aC9yZWFsbXMvc3NiIiwic3ViIjoiOTRkYzViNjEtMTQzNC00Y2IyLWFiNTQtZjU1NDZjZjU2NjFlIiwidHlwIjoiUmVmcmVzaCIsImF6cCI6InplcHBlbGluIiwibm9uY2UiOiIzTnQ2MEk2Sm5POFNaeDB1aFNHZ3g4dmdwTDRDUEVJb2Q3MUczOGFoN2hZIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiYjNjOTE2M2ItMjE0NS00ZjQ1LTg5NGYtNjc5MTA5NjQ4ZTQ2IiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIn0.MsgxGJCCklagRNfDCgjtxo9rm1HTGGkiOTLpharZ1Ak")
                .config("spark.ssb.use.local.credentials", "true")
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla_test-2EYA9GVV2-20200114-173727_1534086404");
        this.sqlContext = session.sqlContext();
    }

    private static BlobId createBucketTestFile(byte[] bytes) {
        BlobId blobId = BlobId.of(bucket, namespace + "/" + version + "/" + UUID.randomUUID().toString() + ".dat");
        getStorage().create(BlobInfo.newBuilder(blobId).build(), bytes, Storage.BlobTargetOption.doesNotExist());
        System.out.println("Blob created: " + blobId.toString());
        return blobId;
    }

    private static Storage getStorage() {
        final GoogleCredentials credentials = GoogleCredentialsFactory.createCredentialsDetails(true,
                StorageScopes.DEVSTORAGE_FULL_CONTROL).getCredentials();
        return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    @Test
    public void testReadWithId() {
        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();
    }

    @Test
    public void testReadWrite() {
        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        System.out.println(tempDirectory);
        dataset.write().mode(SaveMode.Overwrite).save(tempDirectory + "/out.parquet");
    }

    @Test
    public void testReadFromBucket() throws InterruptedException {
        server.enqueue(new MockResponse()
                .setBody(ProtobufJsonUtils.toString(ReadLocationResponse.newBuilder()
                        .setAccessAllowed(true)
                        .setParentUri("gs://" + blobId.getBucket())
                        .setVersion(String.valueOf(version))
                        .build()))
                .setResponseCode(200));

        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
                .load("test/dapla/namespace");

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        final ReadLocationRequest readLocationRequest = ProtobufJsonUtils.toPojo(
                server.takeRequest().getBody().readByteString().utf8(), ReadLocationRequest.class);
        assertThat(readLocationRequest.getPath()).isEqualTo("test/dapla/namespace");
        assertThat(readLocationRequest.getSnapshot()).isEqualTo(0L);
    }


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    @Ignore("Fails from Maven")
    public void testUnauthorizedReadShouldFail() {
        server.enqueue(new MockResponse().setResponseCode(403));
        thrown.expectMessage("Din bruker dapla_test har ikke tilgang til test/dapla/namespace");
        sqlContext.read()
                .format("gsim")
                .load("test/dapla/namespace");
    }

    @Test
    public void testWriteBucket() throws InterruptedException {
        publisher.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(
                new MockResponse().setBody(ProtobufJsonUtils.toString(WriteLocationResponse.newBuilder()
                        .setAccessAllowed(true)
                        .setValidMetadataJson(ByteString.copyFromUtf8("{\n" +
                                "  \"id\": {\n" +
                                "    \"path\": \"/test/dapla/namespace\",\n" +
                                "    \"version\": \"" + System.currentTimeMillis() + "\"\n" +
                                "  },\n" +
                                "  \"valuation\": \"INTERNAL\",\n" +
                                "  \"state\": \"INPUT\",\n" +
                                "  \"parentUri\": \"gs://" + blobId.getBucket() + "\",\n" +
                                "  \"createdBy\": \"junit\"\n" +
                                "}"))
                        .setMetadataSignature(ByteString.copyFromUtf8("some-junit-signature"))
                        .build()))
                        .setResponseCode(200)
        );
        server.enqueue(
                new MockResponse().setBody(ProtobufJsonUtils.toString(WriteAccessTokenResponse.newBuilder()
                        .setAccessToken("junit-test-token")
                        .setExpirationTime(System.currentTimeMillis() + 1000 * 60 * 60) // +1 Hour
                        .build()))
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

        final WriteLocationRequest writeLocationRequest = ProtobufJsonUtils.toPojo(
                server.takeRequest().getBody().readByteString().utf8(), WriteLocationRequest.class);
        assertThat(writeLocationRequest.getMetadataJson()).isEqualTo("{\n" +
                "  \"id\": {\n" +
                "    \"path\": \"/test/dapla/namespace\",\n" +
                "    \"version\": \"" + version + "\"\n" +
                "  },\n" +
                "  \"valuation\": \"INTERNAL\",\n" +
                "  \"state\": \"INPUT\"\n" +
                "}");

        final WriteAccessTokenRequest writeAccessTokenRequest = ProtobufJsonUtils.toPojo(
                server.takeRequest().getBody().readByteString().utf8(), WriteAccessTokenRequest.class);
        assertThat(writeAccessTokenRequest.getMetadataJson()).isEqualTo("{}");
        assertThat(writeAccessTokenRequest.getMetadataSignature()).isEqualTo("some-junit-signature");
    }
}