package no.ssb.dapla.spark.catalog;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.api.services.storage.StorageScopes;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import no.ssb.dapla.spark.sql.TableReader;
import no.ssb.dapla.spark.sql.TableWriter;
import com.google.common.collect.ImmutableMap;
import no.ssb.dapla.gcs.connector.GoogleHadoopFileSystemExt;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.gcs.token.SparkAccessTokenProvider;
import no.ssb.dapla.service.CatalogClient;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.spark.plugin.GsimDatasourceGCSTest;
import no.ssb.dapla.spark.plugin.GsimDatasourceLocalFSTest;
import no.ssb.dapla.spark.plugin.SparkOptions;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.Map;

import static java.time.temporal.ChronoUnit.*;
import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assume.*;

@Ignore
public class CustomSparkCatalogTest {

    private MockWebServer dataAccessMockServer;
    private MockWebServer catalogMockServer;
    private SparkSession session;
    private GoogleCredentialsDetails credentials;

    @Before
    public void setUp() throws IOException {
        this.catalogMockServer = new MockWebServer();
        this.catalogMockServer.start();
        HttpUrl catalogUrl = catalogMockServer.url("/catalog/");

        this.dataAccessMockServer = new MockWebServer();
        this.dataAccessMockServer.start();
        HttpUrl dataAccessUrl = dataAccessMockServer.url("/data-access/");

        this.credentials = GoogleCredentialsFactory.createCredentialsDetails(true,
                StorageScopes.DEVSTORAGE_READ_WRITE);
        // Read the unit dataset json example.
        session = SparkSession.builder()
                .appName(GsimDatasourceLocalFSTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                //.config(CatalogClient.CONFIG_CATALOG_URL, catalogUrl.toString())
                .config(CatalogClient.CONFIG_CATALOG_URL, "http://localhost:20110")
                //.config(DataAccessClient.CONFIG_DATA_ACCESS_URL, dataAccessUrl.toString())
                .config(DataAccessClient.CONFIG_DATA_ACCESS_URL, "http://localhost:20140")
                .config("spark.hadoop.fs.gs.auth.access.token.provider.impl", SparkAccessTokenProvider.class.getCanonicalName())
                .config("spark.hadoop.fs.gs.impl", GoogleHadoopFileSystemExt.class.getCanonicalName())
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                //.config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                //.config("spark.sql.catalog.spark_catalog.type", "hadoop")
                .config("spark.sql.catalog.ssb_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.ssb_catalog.catalog-impl", CustomSparkCatalog.class.getCanonicalName())
                //.config("spark.sql.catalog.ssb_catalog.type", "hadoop")
                //.config("spark.sql.catalog.ssb_catalog.uri", "thrift://localhost:9083")
                //.config("iceberg.engine.hive.enabled", "true")
                //.config("spark.sql.warehouse.dir", "/tmp/hive-warehouse")
                //.config("hive.metastore.uris", "thrift://localhost:9083")
                //.config("hive.metastore.warehouse.dir", "/tmp/hive-warehouse")
                //.config("spark.sql.catalog.ssb_catalog.warehouse", "/tmp/spark-warehouse")
                //.config("spark.sql.catalog.ssb_catalog.warehouse", "gs://ssb-dev-md-test/spark-warehouse")
                //.config("spark.sql.catalog.ssb_catalog.io-impl", CustomFileIO.class.getCanonicalName())

                //.config(SparkOptions.ACCESS_TOKEN, credentials.getAccessToken())
                //.config(SparkOptions.ACCESS_TOKEN_EXP, credentials.getExpirationTime())
                //.enableHiveSupport()
                .config("spark.ssb.access", JWT.create()
                        .withClaim("preferred_username", "kim.gaarder")
                        .withExpiresAt(Date.from(Instant.now().plus(1, HOURS)))
                        .sign(Algorithm.HMAC256("secret")))
                .getOrCreate();
    }

    @After
    public void tearDown() throws Exception {
        session.stop();
    }

    static String READ_LOCATION_RESPONSE = "{\"accessAllowed\": \"true\", \"accessToken\": \"myToken\", \"expirationTime\": \"1580828806046\", \"parentUri\": \"///tmp/spark-datastore\", \"version\": \"1580828806046\"}";
    static String WRITE_LOCATION_RESPONSE = "{\"accessAllowed\": \"true\", \"accessToken\": \"myToken\", \"expirationTime\": \"1580828806046\", \"parentUri\": \"///tmp/spark-datastore\"}";

    @Test
    public void testCreateCatalog() {
        dataAccessMockServer.enqueue(new MockResponse()
                .setBody(WRITE_LOCATION_RESPONSE)
                .setResponseCode(200));
        session.sql("CREATE TABLE ssb_catalog.felles.test.ssb_table (id bigint COMMENT 'unique id', data string) \n" +
                "USING iceberg \n" +
                "COMMENT 'Funny table'\n" +
                "TBLPROPERTIES ('valuation' = 'INTERNAL', 'state' = 'INPUT') \n" +
                "LOCATION '/tmp/spark-runtime/felles/test/ssb_table'");
        /*
                "  'write.object-storage.path'='no.ssb.dapla.spark.catalog.CustomLocationProvider'\n" +
                "  'write.object-storage.enabled'='no.ssb.dapla.spark.catalog.CustomLocationProvider'\n" +
         */
        dataAccessMockServer.enqueue(new MockResponse()
                .setBody(WRITE_LOCATION_RESPONSE)
                .setResponseCode(200));
        dataAccessMockServer.enqueue(new MockResponse()
                .setBody(WRITE_LOCATION_RESPONSE)
                .setResponseCode(200));
        session.sql("INSERT INTO ssb_catalog.felles.test.ssb_table VALUES (1, 'a'), (2, 'b')");
    }

    @Test
    public void testHive() throws IOException {
        //session.sql("CREATE DATABASE IF NOT EXISTS felles LOCATION '/tmp/hive-metastore/felles.db'");
        //session.sql("CREATE EXTERNAL TABLE IF NOT EXISTS spark_tests.s3_table_1 (key INT, value STRING) STORED AS PARQUET LOCATION 's3a://spark/s3_table_1'");

        File tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = GsimDatasourceGCSTest.class.getResourceAsStream("data/dataset.parquet");
        Path parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);

        Dataset<Row> dataset = session.sqlContext().read()
                .load(parquetFile.toString());

        dataset.write()
                .format("iceberg")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                //.option("path", "gs://ssb-dev-md-test/spark-warehouse/felles/test/roblix")
                .option("path", "/tmp/where/do/you/go")
                .saveAsTable("ssb_catalog.felles.TestTable");
    }

    @Test
    public void testSaveAsTableLikePython() throws IOException {
        File tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = GsimDatasourceGCSTest.class.getResourceAsStream("data/dataset.parquet");
        Path parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);

        Dataset<Row> dataset = session.sqlContext().read()
                .load(parquetFile.toString());

        // Write path for Spark 3.0 (df.writeTo) is not yet available in Pyspark
        // Thanks to: https://projectnessie.org/tools/spark/#spark3
        // first instantiate the catalog
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(session.sparkContext().hadoopConfiguration());
        catalog.initialize("python-catalog", ImmutableMap.of("warehouse", "/tmp/spark-warehouse"));

        // Creating table by first creating a table name with namespace
        TableIdentifier region_name = TableIdentifier.parse("testing.region");

        // next create the schema
        Schema schema = SparkSchemaUtil.convert(dataset.schema());

        // and the partition
        PartitionSpec region_spec = PartitionSpec.unpartitioned();

        // finally create or replace the table
        Transaction transaction = catalog.newReplaceTableTransaction(region_name, schema, region_spec, ImmutableMap.of("valuation", "INTERNAL", "state", "INPUT",
                "write.folder-storage.path", "gs://ssb-dev-md-test/spark-warehouse/felles/testing/region"), true);
        transaction.commitTransaction();

        session.sqlContext().conf().setConfString(SparkOptions.ACCESS_TOKEN, credentials.getAccessToken());
        session.sqlContext().conf().setConfString(SparkOptions.ACCESS_TOKEN_EXP, Long.toString(credentials.getExpirationTime()));
        dataset.write().format("iceberg").mode("overwrite").save("ssb_catalog.testing.region");

    }

    @Test
    public void testSaveAsTableLikePythonCustomImpl() throws IOException {
        File tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = GsimDatasourceGCSTest.class.getResourceAsStream("data/dataset.parquet");
        Path parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);

        Dataset<Row> dataset = session.sqlContext().read()
                .load(parquetFile.toString());

        // Write path for Spark 3.0 (df.writeTo) is not yet available in Pyspark
        // Thanks to: https://projectnessie.org/tools/spark/#spark3
        // first instantiate the catalog
        CustomSparkCatalog catalog = new CustomSparkCatalog(session.sparkContext().getConf());
        catalog.setConf(session.sparkContext().hadoopConfiguration());
        catalog.initialize("python-catalog", ImmutableMap.of("warehouse", "/tmp/spark-warehouse"));

        // Creating table by first creating a table name with namespace
        TableIdentifier region_name = TableIdentifier.parse("testing.region");

        if (!catalog.tableExists(region_name)) {
            // next create the schema
            Schema schema = SparkSchemaUtil.convert(dataset.schema());

            // and the partition
            PartitionSpec region_spec = PartitionSpec.unpartitioned();
            catalog.createTable(region_name, schema, region_spec, ImmutableMap.of("valuation", "INTERNAL", "state", "INPUT",
                    "write.folder-storage.path", "gs://ssb-dev-md-test/spark-warehouse/felles/testing/region"));
        }

        dataset.write().format("iceberg").mode("append").save("ssb_catalog.testing.region");
        session.sqlContext().read().format("iceberg").table("ssb_catalog.testing.region").show(false);
        session.sqlContext().table("ssb_catalog/testing/region").show(false);
    }

    @Test
    public void testSaveAsTable() throws IOException {
        File tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = GsimDatasourceGCSTest.class.getResourceAsStream("data/dataset.parquet");
        Path parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);

        Dataset<Row> dataset = session.sqlContext().read()
                .load(parquetFile.toString());

        /*
        dataset.write()
                .format("iceberg")
                .mode(SaveMode.Overwrite)
                .option("valuation", "INTERNAL")
                .option("state", "INPUT")
                .option("path", "gs://ssb-dev-md-test/spark-warehouse/felles/test/roblix")
                .saveAsTable("ssb_catalog.default.roblix");
         */

        // DataFrameWriterV2 kan bruke tableProperty for å skrive til en annen path enn warehouse path
        /*
        dataset.select("PERSON_ID", "MUNICIPALITY").writeTo("ssb_catalog.felles.test.roblix")
                .using("iceberg")
                .tableProperty("valuation", "INTERNAL")
                .tableProperty("state", "INPUT")
                .tableProperty("write.folder-storage.path", "gs://ssb-dev-md-test/spark-data/felles/test/mytable")
                .tableProperty("write.metadata.path", "gs://ssb-dev-md-test/spark-data/felles/test/mytable/.catalog")
                .createOrReplace();

        session.sqlContext().table("ssb_catalog.felles.test.roblix").show(false);
    */
        dataAccessMockServer.enqueue(new MockResponse()
                .setBody(WRITE_LOCATION_RESPONSE)
                .setResponseCode(200));
        dataAccessMockServer.enqueue(new MockResponse()
                .setBody(WRITE_LOCATION_RESPONSE)
                .setResponseCode(200));

        TableWriter.forDataset(dataset)
                .property("valuation", "INTERNAL")
                .property("state", "INPUT")
                .writeTo("/skatt/test/iceberg/bjornandre")
                .createOrReplace();

        //TableReader.forSession(session).readFrom("/felles/test/roblix").show();

    }
    @Test
    public void testLoadTable() throws IOException {
        dataAccessMockServer.enqueue(new MockResponse()
                .setBody(READ_LOCATION_RESPONSE)
                .setResponseCode(200));
        //TableReader.forSession(session).readFrom("/felles/test/roblix").show();
        //session.sqlContext().table("ssb_catalog.testing.region").show(false);
        //session.sqlContext().sql("show create table test_db1.TestTable").show(false);
        session.sqlContext().sql("DESCRIBE EXTENDED ssb_catalog.felles.test.roblix").show(false);
        //session.sqlContext().sql("SELECT * FROM ssb_catalog.felles.test.roblix.snapshots").show(false);
        //session.sqlContext().sql("CALL ssb_catalog.system.rollback_to_snapshot('felles.test.roblix', 2407205414572021903)");
        //session.sqlContext().read().format("iceberg").table("test_db1.TestTable").show(false);
        //session.sqlContext().read().format("iceberg").option("snapshot-id", "506444440131968391").load("ssb_catalog.felles.test.ssb_table").show(false);
        //session.sql("DESCRIBE HISTORY ssb_catalog.felles.test.ssb_table").show();
    }


    @Test
    public void testListByPrefix() {
        catalogMockServer.enqueue(new MockResponse()
                .setBody("{\n" +
                        "  \"entries\": [{\n" +
                        "    \"path\": \"/skatt/person/test1\",\n" +
                        "    \"timestamp\": \"1585256968006\"\n" +
                        "  }, {\n" +
                        "    \"path\": \"/skatt/person/test2\",\n" +
                        "    \"timestamp\": \"1582719098762\"\n" +
                        "  }]\n" +
                        "}\n")
                .setResponseCode(200));

        session.sql("CREATE TABLE ssb_catalog.felles.test.ssb_table (id bigint, data string) USING iceberg");
        System.out.println(session.catalog().listDatabases());
        //Dataset<Database> dataset = session.catalog().listDatabases();
        session.catalog().createTable("test", "path");
        Dataset<Table> dataset = session.catalog().listTables();

        assertThat(dataset).isNotNull();
        dataset.printSchema();
        dataset.show();
        assertThat(dataset.count()).isEqualTo(1);
    }
}
