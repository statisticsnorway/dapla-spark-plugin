package no.ssb.dapla.service;


import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.spark.plugin.GsimDatasourceTest;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.SparkServiceClient.*;

public class SparkServiceClientTest {

    private SparkConf sparkConf = new SparkConf();
    @Before
    public void setUp() {
        // Mock user read by org.apache.hadoop.security.UserGroupInformation
        System.setProperty("HADOOP_USER_NAME", "dapla-test");

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config(CONFIG_ROUTER_URL , "https://dapla-spark.staging-bip-app.ssb.no/")
                .config(CONFIG_ROUTER_OAUTH_TOKEN_URL , "https://keycloak.staging-bip-app.ssb.no/auth/realms/ssb/protocol/openid-connect/token")
                .config(CONFIG_ROUTER_OAUTH_GRANT_TYPE , "client_credential")
                .config(CONFIG_ROUTER_OAUTH_CLIENT_ID , "api-testing-rune-lind")
                .config(CONFIG_ROUTER_OAUTH_USER_NAME , "api-testing-rune-lind")
                .getOrCreate();

        this.sparkConf.set(CONFIG_ROUTER_URL , "https://dapla-spark.staging-bip-app.ssb.no/");
    }

    @Test
    public void test() {
        SparkServiceClient sparkServiceClient = new SparkServiceClient(this.sparkConf);

        Dataset dataset = sparkServiceClient.getDataset("test-user", "ns");

        System.out.println(dataset.getId());
    }
}