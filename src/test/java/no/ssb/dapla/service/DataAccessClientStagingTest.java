package no.ssb.dapla.service;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.DataAccessClient.*;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;

public class DataAccessClientStagingTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_DATA_ACCESS_URL, "https://data-access.staging-bip-app.ssb.no/");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "https://keycloak.staging-bip-app.ssb.no/auth/realms/ssb/protocol/openid-connect/token");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_ID));
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_SECRET));
    }

    @Test
    @Ignore
    public void testGetAccessToken() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        AccessTokenProvider.AccessToken accessToken = dataAccessClient.getAccessToken("user1", "/skatt/person/rawdata-2019",
                AccessTokenRequest.Privilege.READ);
        System.out.println(accessToken);
    }

    @Test
    @Ignore
    public void testGetLocation() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        LocationResponse location = dataAccessClient.getLocationWithLatestVersion("user1", "/skatt/person/rawdata-2019");
        System.out.println(location);
    }

}