package no.ssb.gsim.spark;

import okhttp3.*;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class OAuth2InterceptorTest {

    private MockWebServer tokenServer;
    private MockWebServer resourceServer;

    @Before
    public void setUp() throws Exception {
        this.tokenServer = new MockWebServer();
        this.resourceServer = new MockWebServer();
        this.tokenServer.start();
        this.resourceServer.start();
    }

    @After
    public void tearDown() throws Exception {
        tokenServer.shutdown();
        resourceServer.shutdown();
    }

    @Test
    public void testPassword() throws IOException, InterruptedException {

        tokenServer.enqueue(new MockResponse().setBody("{\"access_token\":\"letoken\"}"));
        resourceServer.enqueue(new MockResponse().setBody("OK"));

        HttpUrl url = tokenServer.url("/token");
        OAuth2Interceptor interceptor = new OAuth2Interceptor(
                url, OAuth2Interceptor.GrantType.PASSWORD,
                null, null,
                "username", "password"
        );

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(interceptor).build();

        Request request = new Request.Builder()
                .url(resourceServer.url("/resource")) .get().build();
        Response execute = client.newCall(request).execute();

        RecordedRequest tokenRequest = tokenServer.takeRequest();
        assertThat(tokenRequest.getUtf8Body())
                .isEqualTo("username=username&password=password&grant_type=password&scope=openid%20profile%20email");

        RecordedRequest resourceRequest = resourceServer.takeRequest();
        assertThat(resourceRequest.getHeader("Authorization")).isEqualTo("Bearer letoken");
    }
}