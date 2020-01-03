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
    private File tempDirectory;
    private Path parquetFile;
    private static String bucket;
    private static String testFolder;
    private BlobId blobId;

    @BeforeClass
    public static void setupBucketFolder() {
        // Verify the test environment
        if (System.getenv().get(GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE) == null) {
            throw new IllegalStateException(String.format("Missing environment variable: " +
                    GoogleCredentialsFactory.SERVICE_ACCOUNT_KEY_FILE));
        }
        // Setup GCS test bucket
        bucket = Optional.ofNullable(System.getenv().get("DAPLA_SPARK_TEST_BUCKET")).orElse("dev-datalager-store");
        testFolder = "dapla-spark-plugin-" + UUID.randomUUID().toString();
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
    public void setUp() throws Exception {
        // Create temporary folder and copy test data into it.
        tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = this.getClass().getResourceAsStream("data/dataset.parquet");
        parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);
        System.out.println("File created: " + parquetFile.toString());
        blobId = createBucketTestFile(Files.readAllBytes(parquetFile));
        // Mock user read by org.apache.hadoop.security.UserGroupInformation
        System.setProperty("HADOOP_USER_NAME", "dapla-test");

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config("spark.hadoop.fs.gs.delegation.token.binding", BrokerDelegationTokenBinding.class.getCanonicalName())
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sqlContext = session.sqlContext();

    }

    private BlobId createBucketTestFile(byte[] bytes) {
        BlobId blobId = BlobId.of(bucket, testFolder + "/dataset-" + UUID.randomUUID().toString() + ".dat");
        getStorage().create(BlobInfo.newBuilder(blobId).build(), bytes, Storage.BlobTargetOption.doesNotExist());
        System.out.println("Blob created: " + blobId.toString());
        return blobId;
    }

    private static Storage getStorage() {
        final GoogleCredentials credentials = GoogleCredentialsFactory.createCredentialsDetails(true,
                "https://www.googleapis.com/auth/devstorage.full_control").getCredentials();
        return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    @Test
    public void testReadWithId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
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
        dataset.write().format("gsim").mode(SaveMode.Overwrite).save(tempDirectory + "/out.parquet");
    }

    @Test
    public void testReadFromBucket() {
        Dataset<Row> dataset = sqlContext.read()
                //.format("gsim")
                .option("authToken", "test")
                .load("gs://" + blobId.getBucket() + "/" + blobId.getName());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();
    }

    @Test
    public void testReadWithIdAndCallServer() {
        try (SparkPluginTestServer sparkPluginTestServer = new SparkPluginTestServer(9123)) {
            sparkPluginTestServer.start();

            Dataset<Row> dataset = sqlContext.read()
                    .format("gsim")
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