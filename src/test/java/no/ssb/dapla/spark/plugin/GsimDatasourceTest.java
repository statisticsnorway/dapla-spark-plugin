package no.ssb.dapla.spark.plugin;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.spark.protobuf.HelloRequest;
import no.ssb.dapla.spark.protobuf.HelloResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;
import no.ssb.dapla.spark.router.SparkServiceRouter;
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
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.*;

public class GsimDatasourceTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private static File tempDirectory;
    private static Path parquetFile;
    private static String bucket;
    private static String testFolder;
    private static BlobId blobId;

    @BeforeClass
    public static void setupBucketFolder() throws Exception {
        // Verify the test environment
        if (System.getenv().get(GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE) == null) {
            throw new IllegalStateException(String.format("Missing environment variable: " +
                    GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE));
        }
        // Setup GCS test bucket
        bucket = Optional.ofNullable(System.getenv().get("DAPLA_SPARK_TEST_BUCKET")).orElse("dev-datalager-store");
        testFolder = "dapla-spark-plugin-" + UUID.randomUUID().toString();
        // Create temporary folder and copy test data into it.
        tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = GsimDatasourceTest.class.getResourceAsStream("data/dataset.parquet");
        parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);
        System.out.println("File created: " + parquetFile.toString());
        blobId = createBucketTestFile(Files.readAllBytes(parquetFile));
        setUserPermissionForTest("dapla-test", "dapla.namespace");
    }

    @AfterClass
    public static void clearBucketFolder() {
        final Storage storage = getStorage();
        Page<Blob> page = storage.list(bucket, Storage.BlobListOption.prefix(testFolder + "/"));
        BlobId[] blobs = StreamSupport.stream(page.iterateAll().spliterator(), false).map(BlobInfo::getBlobId).collect(Collectors.toList()).toArray(new BlobId[0]);
        if (blobs.length > 0) {
            List<Boolean> deletedList = storage.delete(blobs);
            for (Boolean deleted : deletedList) {
                if (!deleted) {
                    throw new RuntimeException("Unable to delete blob in bucket");
                }
            }
        }
    }

    @After
    public void tearDown() {
        sparkContext.stop();
    }

    @Before
    public void setUp() {
        // Mock user read by org.apache.hadoop.security.UserGroupInformation
        System.setProperty("HADOOP_USER_NAME", "dapla-test");

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config("fs.gs.impl.disable.cache", true)
                .config("spark.hadoop.fs.gs.delegation.token.binding", BrokerDelegationTokenBinding.class.getCanonicalName())
                //.config("spark.hadoop.fs.gs.auth.access.token.provider.impl", BrokerAccessTokenProvider.class.getCanonicalName())
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sparkContext.setLocalProperty("spark.jobGroup.id",
                "zeppelin-dapla-test-2EYA9GVV2-20200114-173727_1534086404");
        this.sqlContext = session.sqlContext();
    }

    private static BlobId createBucketTestFile(byte[] bytes) {
        BlobId blobId = BlobId.of(bucket, testFolder + "/dataset-" + UUID.randomUUID().toString() + ".dat");
        getStorage().create(BlobInfo.newBuilder(blobId).build(), bytes, Storage.BlobTargetOption.doesNotExist());
        System.out.println("Blob created: " + blobId.toString());
        return blobId;
    }

    private static void setUserPermissionForTest(String userId, String namespace) {
        final String location = "gs://" + blobId.getBucket() + "/" + blobId.getName();
        // Write a mock mapping from namespace to location, and set user permission
        SparkServiceRouter.getInstance("gs://" + blobId.getBucket()).write(SaveMode.Overwrite, userId, namespace, location);
    }

    private static Storage getStorage() {
        final GoogleCredentials credentials = GoogleCredentialsFactory.createCredentialsDetails(true,
                "https://www.googleapis.com/auth/devstorage.full_control").getCredentials();
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
    public void testReadFromBucket() {
        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
                .load("dapla.namespace");

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();
    }

    @Test
    @Ignore("Figure out why this fails")
    public void testWriteBucket() {
        try {
            Dataset<Row> dataset = sqlContext.read()
                    .load(parquetFile.toString());
            dataset.write()
                    .format("gsim")
                    .mode(SaveMode.Overwrite)
                    .save("dapla.namespace");
            assertThat(dataset).isNotNull();
            assertThat(dataset.isEmpty()).isFalse();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Test
    @Ignore("Dapla plugin server is not yet implemented")
    public void testReadWithIdAndCallServer() {
        try (SparkPluginTestServer sparkPluginTestServer = new SparkPluginTestServer(9123)) {
            sparkPluginTestServer.start();

            Dataset<Row> dataset = sqlContext.read()
                    .option("uri_to_dapla_plugin_server", "localhost:9123")
                    .load(parquetFile.toString());

            assertThat(dataset).isNotNull();
            assertThat(dataset.isEmpty()).isFalse();

            List<String> responses = sparkPluginTestServer.getResponses();
            assertThat(responses.size()).isEqualTo(1);
            assertThat(responses.get(0)).startsWith("Hello from dapla-spark-plugin!");
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class SparkPluginTestServer implements AutoCloseable {
        private final Server server;
        private AtomicBoolean isRunning = new AtomicBoolean(false);

        public List<String> getResponses() {
            return responses;
        }

        private ArrayList<String> responses = new ArrayList<>();

        public SparkPluginTestServer(int port) {
            server = ServerBuilder.forPort(port).addService(new SparkPluginService())
                    .build();
        }

        public void start() throws IOException {
            server.start();
            isRunning.set(true);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    if (isRunning.get()) {
                        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                        System.err.println("*** shutting down gRPC server since JVM is shutting down");
                        SparkPluginTestServer.this.stop();
                        System.err.println("*** server shut down");
                    } else {
                        System.out.println("Is already shutting down");
                    }
                }
            });
        }

        @Override
        public void close() throws InterruptedException {
            isRunning.set(false);
            System.out.println("SparkPluginTestServer - close");
            stop();
            blockUntilShutdown();
        }

        /**
         * Stop serving requests and shutdown resources.
         */
        public void stop() {
            if (server != null) {
                server.shutdown();
            }
        }

        /**
         * Await termination on the main thread since the grpc library uses daemon threads.
         */
        private void blockUntilShutdown() throws InterruptedException {
            if (server != null) {
                server.awaitTermination();
            }
        }

        private class SparkPluginService extends SparkPluginServiceGrpc.SparkPluginServiceImplBase {

            @Override
            public void sayHello(HelloRequest request, StreamObserver<HelloResponse> responseObserver) {
                String greeting = request.getGreeting();
                System.out.println(greeting);
                responses.add(greeting);
                try {
                    responseObserver.onNext(HelloResponse.newBuilder().setReply("hello from server").build());
                    responseObserver.onCompleted();

                } catch (Exception ex) {
                    responseObserver.onError(new StatusException(Status.fromThrowable(ex)));
                }
            }
        }
    }
}