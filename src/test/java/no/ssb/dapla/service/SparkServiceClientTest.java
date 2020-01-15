package no.ssb.dapla.service;


import no.ssb.dapla.catalog.protobuf.Dataset;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;

import static no.ssb.dapla.service.SparkServiceClient.*;

public class SparkServiceClientTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_ROUTER_URL, "https://dapla-spark.staging-bip-app.ssb.no/");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "https://keycloak.staging-bip-app.ssb.no/auth/realms/ssb/protocol/openid-connect/token");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_GRANT_TYPE, "client_credential");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, "api-testing-rune-lind");
    }

    @Test
    public void test() {
        SparkServiceClient sparkServiceClient = new SparkServiceClient(this.sparkConf);

        Dataset dataset = sparkServiceClient.getDataset("test-user", "ns");

        System.out.println(dataset.getId());
    }
}