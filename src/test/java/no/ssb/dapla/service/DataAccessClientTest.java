package no.ssb.dapla.service;


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
        this.sparkConf.set("spark.ssb.access", "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJvN3BfZU5GT0RWdGlzSmZ1MlE1WnBnaVRGd1ZUUlBWRktFZjFWSlFiUEpZIn0.eyJqdGkiOiI2NTAxZDIxMC03YzVhLTQyMzktYmU3Yi1jOTg3MDE3YTBmMzkiLCJleHAiOjE1ODMzOTMwNTMsIm5iZiI6MCwiaWF0IjoxNTgzMzkyNzUzLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjI4MDgxL2F1dGgvcmVhbG1zL3NzYiIsImF1ZCI6ImFjY291bnQiLCJzdWIiOiI5NGRjNWI2MS0xNDM0LTRjYjItYWI1NC1mNTU0NmNmNTY2MWUiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJ6ZXBwZWxpbiIsIm5vbmNlIjoiM050NjBJNkpuTzhTWngwdWhTR2d4OHZncEw0Q1BFSW9kNzFHMzhhaDdoWSIsImF1dGhfdGltZSI6MTU4MzM5Mjc1Mywic2Vzc2lvbl9zdGF0ZSI6ImIzYzkxNjNiLTIxNDUtNGY0NS04OTRmLTY3OTEwOTY0OGU0NiIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiaHR0cDovL2xvY2FsaG9zdDoyODAxMCJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsib2ZmbGluZV9hY2Nlc3MiLCJ1bWFfYXV0aG9yaXphdGlvbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7ImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInNjb3BlIjoib3BlbmlkIGVtYWlsIHByb2ZpbGUiLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsIm5hbWUiOiJLaW0iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJraW0iLCJnaXZlbl9uYW1lIjoiS2ltIiwiZW1haWwiOiJkYXBsYS1raW1Ac3NiLm5vIn0.fQZGvvjCsvipcp5MLZTDCu4qseraVxW2PMIwfCpi7aEiCRP3NLsnumKshyrzYrq4lNw0hACGnRd9__aEBG7cgF8QFi2r8me3eG3Q3syxd_UEmxDYwSnSTeune745R2mBIIKWq5fpd9ZYUSnUyEzGhvvyGYWn84LDZphUTrryok-yRRfb-XzhpYTCizYesOtpa2WMwh_b1qtVo1nd3rC2MFR1rl7RvkokNAcB6clRs23AbPPYJvDnXZjmKujbWdzDuWJOnVX0OYzExoe7zUGm693P9EF9JceOI1PhiURiKStVxlYqhNPo8BwR6TxkcJ2BF5I3XeXYd3nvpwFEjzNTLw");
        this.sparkConf.set("spark.ssb.refresh", "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5MGJjMWIxNi1lNWIxLTQ3ZmQtYjNmOC1lN2NjMmQ1ZDZmNmYifQ.eyJqdGkiOiJjZGM0NmRjNS1kZTUyLTRhZDQtOGFhYS05ZGQwMzIwMDMxMmQiLCJleHAiOjE1ODMzOTQ1NTMsIm5iZiI6MCwiaWF0IjoxNTgzMzkyNzUzLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjI4MDgxL2F1dGgvcmVhbG1zL3NzYiIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6MjgwODEvYXV0aC9yZWFsbXMvc3NiIiwic3ViIjoiOTRkYzViNjEtMTQzNC00Y2IyLWFiNTQtZjU1NDZjZjU2NjFlIiwidHlwIjoiUmVmcmVzaCIsImF6cCI6InplcHBlbGluIiwibm9uY2UiOiIzTnQ2MEk2Sm5POFNaeDB1aFNHZ3g4dmdwTDRDUEVJb2Q3MUczOGFoN2hZIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiYjNjOTE2M2ItMjE0NS00ZjQ1LTg5NGYtNjc5MTA5NjQ4ZTQ2IiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6Im9wZW5pZCBlbWFpbCBwcm9maWxlIn0.MsgxGJCCklagRNfDCgjtxo9rm1HTGGkiOTLpharZ1Ak");
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