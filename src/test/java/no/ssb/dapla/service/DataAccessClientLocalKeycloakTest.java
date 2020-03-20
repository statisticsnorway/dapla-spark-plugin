package no.ssb.dapla.service;

import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.DataAccessClient.*;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;

public class DataAccessClientLocalKeycloakTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_DATA_ACCESS_URL, "http://localhost:20140/");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "http://localhost:28081/auth/realms/ssb/protocol/openid-connect/token");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, "zeppelin");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, "ed48ee94-fe9d-4096-b069-9626a52877f2");
        this.sparkConf.set(SPARK_SSB_ACCESS_TOKEN, System.getenv("spark.ssb.access"));
        this.sparkConf.set(SPARK_SSB_REFRESH_TOKEN, System.getenv("spark.ssb.refresh"));
    }

    @Test
    @Ignore
    public void testGetAccessToken() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/skatt/person/rawdata-2019")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
        ReadAccessTokenResponse readAccessTokenResponse = dataAccessClient.readAccessToken(ReadAccessTokenRequest.newBuilder()
                .setPath("/skatt/person/rawdata-2019")
                .setVersion(readLocationResponse.getVersion())
                .build());
        System.out.println(readAccessTokenResponse.getAccessToken());
    }

    @Test
    @Ignore
    public void testGetLocation() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/skatt/person/rawdata-2019")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
        System.out.println(readLocationResponse);
    }

}