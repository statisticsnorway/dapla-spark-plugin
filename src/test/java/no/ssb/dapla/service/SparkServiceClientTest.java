package no.ssb.dapla.service;


import no.ssb.dapla.catalog.protobuf.Dataset;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.SparkServiceClient.*;

public class SparkServiceClientTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_ROUTER_URL, "https://dapla-spark.staging-bip-app.ssb.no/");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "https://keycloak.staging-bip-app.ssb.no/auth/realms/ssb/protocol/openid-connect/token");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_GRANT_TYPE, "client_credential");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_ID));
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_SECRET));
    }

    @Test
    public void test() {
        SparkServiceClient sparkServiceClient = new SparkServiceClient(this.sparkConf);

        Dataset dataset = sparkServiceClient.getDataset("rune.lind@ssbmod.net", "skatt.person.2019.rawdata");
        System.out.println(dataset);

        //Dataset dataset = sparkServiceClient.createDataset("rune.lind@ssbmod.net", SaveMode.ErrorIfExists, "skatt.person.2018.rawdata");
        //System.out.println(dataset);
    }
}