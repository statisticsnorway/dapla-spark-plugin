package no.ssb.gsim.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static no.ssb.gsim.spark.GsimDatasource.*;

public class GsimDatasourceTest {

    public static final String UNIT_DATASET_ID = "b9c10b86-5867-4270-b56e-ee7439fe381e";
    private SparkConf config;
    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private File tempDirectory;

    @After
    public void tearDown() throws Exception {
        sparkContext.stop();
    }

    @Before
    public void setUp() throws Exception {
        this.config = new SparkConf()
                .setAppName("testing provider")
                .setMaster("local");

        tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();

        config.set(CONFIG_LOCATION_PREFIX, tempDirectory.toString());

        config.set(CONFIG_LDS_OAUTH_TOKEN_URL,
                "https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token");

        config.set(CONFIG_LDS_URL, "https://lds-c.staging.ssbmod.net/ns/");
        config.set(CONFIG_LDS_OAUTH_CLIENT_ID, "lds-c-postgres-gsim");
        config.set(CONFIG_LDS_OAUTH_USER_NAME, "api-user-3");
        config.set(CONFIG_LDS_OAUTH_PASSWORD, "890e9e58-b1b5-4705-a557-69c19c89dbcf");

        this.sparkContext = new SparkContext(config);
        this.sqlContext = new SQLContext(sparkContext);
    }

    @Test
    public void testReadWithId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://" + UNIT_DATASET_ID);
        dataset.show();
    }

    @Test
    public void testReadWithPathAndId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://UnitDataset/" + UNIT_DATASET_ID);
        dataset.show();
    }

    @Test
    public void testReadWithPrefixAndId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://lds-postgres.staging.ssbmod.net/" + UNIT_DATASET_ID);
        dataset.show();
    }

    @Test
    public void testReadWithPrefixAndPathAndId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://lds-postgres.staging.ssbmod.net/UnitDataset/" + UNIT_DATASET_ID);
        dataset.show();
    }

    @Test
    public void testWriteWithId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://" + UNIT_DATASET_ID);
        dataset.show();

        Dataset<Row> dataset2 = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://" + UNIT_DATASET_ID);
        Dataset<Row> distinct = dataset2.distinct();

        //dataset.write().insertInto("test");
        dataset.write().format("no.ssb.gsim.spark").mode(SaveMode.Append).save("lds+gsim://" + UNIT_DATASET_ID);

    }
}