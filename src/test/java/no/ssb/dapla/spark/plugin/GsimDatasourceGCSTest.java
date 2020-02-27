package no.ssb.dapla.spark.plugin;

import com.google.api.gax.paging.Page;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.gcs.connector.GoogleHadoopFileSystemExt;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.NoOpMetadataWriter;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.Dispatcher;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.*;

public class GsimDatasourceGCSTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private static File tempDirectory;
    private static Path parquetFile;
    private static String bucket;
    private static String namespace = "test/dapla/namespace";
    private static String version = UUID.randomUUID().toString();
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
                .config(MetadataPublisherClient.CONFIG_METADATA_PUBLISHER_URL, publisherUrl.toString())
                .config("spark.ssb.dapla.metadata.writer", NoOpMetadataWriter.class.getCanonicalName())
                .config("spark.hadoop.fs.gs.impl", GoogleHadoopFileSystemExt.class.getCanonicalName())
                .config("spark.hadoop.fs.gs.delegation.token.binding", BrokerDelegationTokenBinding.class.getCanonicalName())
                //.config("spark.hadoop.fs.gs.auth.access.token.provider.impl", BrokerAccessTokenProvider.class.getCanonicalName())
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
    public void testUserId() {
        GsimDatasource gsimDatasource = new GsimDatasource();
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-bjorn-andre.skaar@ssbmod.net-2EYA9GVV2-20200114-173727_1534086404");
        assertThat(gsimDatasource.getUserId(this.sparkContext)).isEqualTo("bjorn-andre.skaar@ssbmod.net");
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-rune.lind@ssbmod.net-2EWU778YE-20200113-130142_730486519");
        assertThat(gsimDatasource.getUserId(this.sparkContext)).isEqualTo("rune.lind@ssbmod.net");
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-user1-sample-pseudo-20200114-130807_128334940");
        assertThat(gsimDatasource.getUserId(this.sparkContext)).isEqualTo("user1");
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla_test-2EYA9GVV2-20200114-173727_1534086404");
        assertThat(gsimDatasource.getUserId(this.sparkContext)).isEqualTo("dapla_test");
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla-test-2EYA9GVV2-20200114-173727_1534086404");
        assertThat(gsimDatasource.getUserId(this.sparkContext)).isEqualTo("dapla");
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
        LocationResponse mockResponse = createMockResponse("gs://" + blobId.getBucket(), version);

        server.enqueue(new MockResponse().setBody(ProtobufJsonUtils.toString(mockResponse)).setResponseCode(200));
        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
                .load("test/dapla/namespace");

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        final LocationRequest actual = ProtobufJsonUtils.toPojo(server.takeRequest().getBody().readByteString().utf8(),
                LocationRequest.class);
        assertThat(actual.getUserId()).isEqualTo("dapla_test");
        assertThat(actual.getPath()).isEqualTo("test/dapla/namespace");
        assertThat(actual.getSnapshot()).isEqualTo(0L);
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
    public void testWriteBucket() {
        publisher.enqueue(new MockResponse().setResponseCode(200));
        server.setDispatcher(new Dispatcher() {
            public MockResponse dispatch(RecordedRequest request) {
                final LocationRequest body = ProtobufJsonUtils.toPojo(request.getBody().readByteString().utf8(),
                        LocationRequest.class);
                return new MockResponse().setBody(ProtobufJsonUtils.toString(createMockResponse(
                        "gs://" + blobId.getBucket(), Long.toString(body.getSnapshot()))));
            }
        });

        Dataset<Row> dataset = sqlContext.read()
                .load(parquetFile.toString());
        dataset.write()
                .format("gsim")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                .save("test/dapla/namespace");
        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();
    }

    private LocationResponse createMockResponse(String parentUri, String version) {
        return LocationResponse.newBuilder()
                .setParentUri(parentUri)
                .setVersion(version).build();
    }

}