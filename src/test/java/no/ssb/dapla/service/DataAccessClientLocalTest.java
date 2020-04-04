package no.ssb.dapla.service;

import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.DataAccessClient.CONFIG_DATA_ACCESS_URL;
import static org.assertj.core.api.Assertions.assertThat;

public class DataAccessClientLocalTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_DATA_ACCESS_URL, "http://localhost:20140/");
    }

    @Test
    @Ignore
    public void testGetLocation() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/skatt/person/rawdata-2019")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
        assertThat(readLocationResponse.getAccessAllowed()).isTrue();
        System.out.println(readLocationResponse.getAccessToken());
    }

}