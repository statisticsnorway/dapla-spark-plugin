package no.ssb.dapla.service;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static no.ssb.dapla.service.SparkServiceClient.CONFIG_ROUTER_URL;

/*
    For manually testing against localstack
 */
public class SparkServiceClientLocalstackTest {

    private SparkConf sparkConf = new SparkConf();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_ROUTER_URL, "http://localhost:20120/");
    }

    @Test
    @Ignore
    public void testRead_WithException() {
        SparkServiceClient sparkServiceClient = new SparkServiceClient(this.sparkConf);

        thrown.expectMessage("En feil har oppstått:");
        thrown.expectMessage("No enum constant no.ssb.dapla.auth.dataset.protobuf.Role.DatasetState.RAWDATA");

        sparkServiceClient.createDataset("user1", SaveMode.Overwrite,
                "skatt.person/testfolder/testdataset", "INTERNAL", "RAWDATA");
    }
}