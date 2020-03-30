package no.ssb.dapla.service;


import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static no.ssb.dapla.service.DataAccessClient.CONFIG_DATA_ACCESS_URL;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_CLIENT_ID;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_CLIENT_SECRET;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_TOKEN_IGNORE_EXPIRY;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_TOKEN_URL;
import static org.assertj.core.api.Assertions.assertThat;

public class DataAccessClientTest {

    private SparkConf sparkConf = new SparkConf();
    private MockWebServer server;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws IOException {
        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/data-access/");
        this.sparkConf.set(CONFIG_DATA_ACCESS_URL, baseUrl.toString());
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "http://localhost:28081/auth/realms/ssb/protocol/openid-connect/token");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, "zeppelin");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, "ed48ee94-fe9d-4096-b069-9626a52877f2");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_IGNORE_EXPIRY, "true");
        this.sparkConf.set("spark.ssb.access", JWT.create().withClaim("preferred_username", "kim").withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.HOURS))).sign(Algorithm.HMAC256("secret")));
        this.sparkConf.set("spark.ssb.refresh", JWT.create().withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.HOURS))).sign(Algorithm.HMAC256("secret")));

    }

    @Test
    public void testGetAccessToken() throws InterruptedException {
        server.enqueue(new MockResponse()
                .setBody("{\"accessAllowed\": \"true\", \"parentUri\": \"file:///hei\", \"version\": \"1580828806046\"}")
                .setResponseCode(200));
        server.enqueue(new MockResponse()
                .setBody("{\"accessToken\": \"myToken\", \"expirationTime\": \"1580828806046\"}")
                .setResponseCode(200));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);

        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/myBucket")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
        assertThat(readLocationResponse.getAccessAllowed()).isTrue();
        ReadAccessTokenResponse readAccessTokenResponse = dataAccessClient.readAccessToken(ReadAccessTokenRequest.newBuilder()
                .setPath("/myBucket")
                .setVersion(readLocationResponse.getVersion())
                .build());

        assertThat(readAccessTokenResponse.getAccessToken()).isEqualTo("myToken");
        assertThat(readAccessTokenResponse.getExpirationTime()).isEqualTo(1580828806046L);

        RecordedRequest recordedRequest = server.takeRequest();
        assertThat(recordedRequest.getBody().readByteString().utf8()).isEqualTo("{\n" +
                "  \"path\": \"/myBucket\"\n" +
                "}");
    }

    @Test
    public void testHandleAccessDenied() {
        server.enqueue(new MockResponse().setResponseCode(403));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        thrown.expectMessage("Din bruker har ikke tilgang");
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/myBucket")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
        assertThat(readLocationResponse.getAccessAllowed()).isFalse();
    }

    @Test
    public void testHandleNotFound() {
        server.enqueue(new MockResponse().setResponseCode(404));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        thrown.expectMessage("Fant ikke datasett");
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/myBucket")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
    }

    @Test
    public void testHandleExceptionFromServer() {
        server.enqueue(new MockResponse().setBody("Message from server").setResponseCode(500));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);

        thrown.expectMessage("En feil har oppstått:");
        thrown.expectMessage("Message from server");

        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/myBucket")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
    }
}