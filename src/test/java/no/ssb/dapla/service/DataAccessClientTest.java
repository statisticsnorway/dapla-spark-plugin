package no.ssb.dapla.service;

import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.DataAccessClient.CONFIG_ROUTER_URL;

public class DataAccessClientTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_ROUTER_URL, "http://localhost:10140/");
    }

    @Test
    @Ignore
    public void getAccessToken() {
        DataAccessClient dataAccessClient = new DataAccessClient(sparkConf);

        AccessTokenResponse accessTokenResponse = dataAccessClient.getAccessToken("user1", "/skatt/person", AccessTokenRequest.Privilege.READ);
    }

    @Test
//    @Ignore
    public void getLocation() {
        DataAccessClient dataAccessClient = new DataAccessClient(sparkConf);

        LocationResponse locationResponse = dataAccessClient.getLocation("user1", "/skatt/person", 0);
    }
}