package no.ssb.dapla.service;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.data.access.protobuf.Privilege;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.DataAccessClient.CONFIG_DATA_ACCESS_URL;

public class DataAccessClientLocalTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_DATA_ACCESS_URL, "http://localhost:20140/");
    }

    @Test
    @Ignore
    public void testGetAccessToken() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        AccessTokenProvider.AccessToken accessToken = dataAccessClient.getAccessToken("/skatt/person/rawdata-2019",
                0, Privilege.READ, null, null);
        System.out.println(accessToken.getToken());
    }

    @Test
    @Ignore
    public void testGetLocation() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        LocationResponse location = dataAccessClient.getReadLocationWithLatestVersion("/skatt/person/rawdata-2019");
        System.out.println(location);
    }

}