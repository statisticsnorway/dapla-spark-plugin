package no.ssb.dapla.service;


import com.google.cloud.hadoop.util.AccessTokenProvider;
import no.ssb.dapla.data.access.protobuf.Privilege;
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
    }

    @Test
    public void testGetAccessToken() throws InterruptedException {
        String mockResult = "{\"accessToken\": \"myToken\", \"expirationTime\": \"1580828806046\"}";
        server.enqueue(new MockResponse().setBody(mockResult).setResponseCode(200));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);

        AccessTokenProvider.AccessToken accessToken = dataAccessClient.getAccessToken("user1", "myBucket",
                0, Privilege.READ, null, null);
        assertThat(accessToken.getToken()).isEqualTo("myToken");
        assertThat(accessToken.getExpirationTimeMilliSeconds()).isEqualTo(1580828806046L);

        RecordedRequest recordedRequest = server.takeRequest();
        assertThat(recordedRequest.getBody().readByteString().utf8()).isEqualTo("{\n" +
                "  \"path\": \"myBucket\"\n" +
                "}");
    }

    @Test
    public void testHandleAccessDenied() {
        server.enqueue(new MockResponse().setResponseCode(403));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        thrown.expectMessage("Din bruker har ikke READ tilgang til myBucket");
        dataAccessClient.getAccessToken("user1", "myBucket", 0, Privilege.READ, null, null);
    }

    @Test
    public void testHandleNotFound() {
        server.enqueue(new MockResponse().setResponseCode(404));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        thrown.expectMessage("Fant ingen location myBucket");
        dataAccessClient.getAccessToken("user1", "myBucket", 0, Privilege.READ, null, null);
    }

    @Test
    public void testHandleExceptionFromServer() {
        server.enqueue(new MockResponse().setBody("Message from server").setResponseCode(500));
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);

        thrown.expectMessage("En feil har oppstått:");
        thrown.expectMessage("Message from server");

        dataAccessClient.getAccessToken("user1", "myBucket", 0, Privilege.READ, null, null);
    }
}