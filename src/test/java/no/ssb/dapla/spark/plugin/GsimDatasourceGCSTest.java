package no.ssb.dapla.spark.plugin;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.api.gax.paging.Page;
import com.google.api.services.storage.StorageScopes;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.gcs.connector.GoogleHadoopFileSystemExt;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.gcs.token.SparkAccessTokenProvider;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.FilesystemMetaDataWriter;
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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.time.temporal.ChronoUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assume.assumeThat;

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
    private static GoogleCredentialsDetails credentials;

    @BeforeClass
    public static void setupBucketFolder() throws Exception {
        assumeThat(System.getenv().get(GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE), notNullValue());

        // Setup GCS test bucket
        bucket = Optional.ofNullable(System.getenv().get("DAPLA_SPARK_TEST_BUCKET")).orElse("ssb-dev-md-test");
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
        if (bucket == null) {
            return;
        }

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
        assumeThat(System.getenv().get(GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE), notNullValue());

        // Mock user read by org.apache.hadoop.security.UserGroupInformation
        System.setProperty("HADOOP_USER_NAME", "dapla_test");

        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/data-access/");

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
                .config("spark.hadoop.fs.gs.auth.access.token.provider.impl", SparkAccessTokenProvider.class.getCanonicalName())
                .config("spark.ssb.dapla.metadata.publisher.project.id", "dapla")
                .config("spark.ssb.dapla.metadata.publisher.topic.name", "file-events-1")
                .config("spark.ssb.dapla.oauth.tokenUrl", "http://localhost:28081/auth/realms/ssb/protocol/openid-connect/token")
                .config("spark.ssb.dapla.oauth.clientId", "zeppelin")
                .config("spark.ssb.dapla.oauth.clientSecret", "ed48ee94-fe9d-4096-b069-9626a52877f2")
                .config("spark.ssb.dapla.oauth.ignoreExpiry", "true")
                .config("spark.ssb.dapla.default.partition.size", 5)
                .config("spark.ssb.username", "kim")
                .config("spark.ssb.access", JWT.create()
                        .withClaim("preferred_username", "kim")
                        .withExpiresAt(Date.from(Instant.now().plus(1, HOURS)))
                        .sign(Algorithm.HMAC256("secret")))
                .config("spark.ssb.access", JWT.create()
                        .withClaim("preferred_username", "kim")
                        .withExpiresAt(Date.from(Instant.now().plus(1, HOURS)))
                        .sign(Algorithm.HMAC256("secret")))
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
        if (credentials == null) {
            credentials = GoogleCredentialsFactory.createCredentialsDetails(true,
                    StorageScopes.DEVSTORAGE_FULL_CONTROL);
        }
        return StorageOptions.newBuilder().setCredentials(credentials.getCredentials()).build().getService();
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
                        .setAccessToken(credentials.getAccessToken())
                        .setExpirationTime(System.currentTimeMillis() + 1000 * 60 * 60) // +1 Hour
                        .build()))
                .setResponseCode(200));

        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
                .load("/test/dapla/namespace");

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        {
            RecordedRequest recordedRequest = server.takeRequest();
            final ReadLocationRequest readLocationRequest = ProtobufJsonUtils.toPojo(
                    recordedRequest.getBody().readByteString().utf8(), ReadLocationRequest.class);
            assertThat(readLocationRequest.getPath()).isEqualTo("/test/dapla/namespace");
            assertThat(readLocationRequest.getSnapshot()).isEqualTo(0L);
        }
    }


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    @Ignore("Fails from Maven")
    public void testUnauthorizedReadShouldFail() {
        server.enqueue(new MockResponse().setResponseCode(403));
        thrown.expectMessage("Din bruker har ikke tilgang");
        sqlContext.read()
                .format("gsim")
                .load("/test/dapla/namespace");
    }

    @Test
    public void testWriteBucket() throws InterruptedException {
        long version = System.currentTimeMillis();
        publisher.enqueue(new MockResponse().setResponseCode(200));
        publisher.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(
                new MockResponse()
                        .setBody(ProtobufJsonUtils.toString(WriteLocationResponse.newBuilder()
                                .setAccessAllowed(true)
                                .setValidMetadataJson(ByteString.copyFromUtf8("{\n" +
                                        "  \"id\": {\n" +
                                        "    \"path\": \"/test/dapla/namespace\",\n" +
                                        "    \"version\": \"" + version + "\"\n" +
                                        "  },\n" +
                                        "  \"valuation\": \"INTERNAL\",\n" +
                                        "  \"state\": \"INPUT\",\n" +
                                        "  \"createdBy\": \"junit\"\n" +
                                        "}"))
                                .setMetadataSignature(ByteString.copyFromUtf8("some-junit-signature"))
                                .setParentUri("gs://" + blobId.getBucket())
                                .setAccessToken(credentials.getAccessToken())
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

    }
}