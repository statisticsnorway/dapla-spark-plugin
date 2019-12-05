package no.ssb.dapla.spark.plugin;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                .url(resourceServer.url("/resource")).get().build();
        Response execute = client.newCall(request).execute();

        RecordedRequest tokenRequest = tokenServer.takeRequest();
        assertThat(tokenRequest.getBody().readByteString().utf8())
                .isEqualTo("" +
                        "username=username&" +
                        "password=password&" +
                        "grant_type=password&" +
                        "scope=openid%20profile%20email" +
                        "");

        RecordedRequest resourceRequest = resourceServer.takeRequest();
        assertThat(resourceRequest.getHeader("Authorization")).isEqualTo("Bearer letoken");
    }

    @Test
    public void testInvalidCombinations() throws IOException, InterruptedException {


        HttpUrl url = HttpUrl.get("http://localhost/");
        new OAuth2Interceptor(
                url, OAuth2Interceptor.GrantType.CLIENT_CREDENTIAL,
                "client", "secret",
                null, null
        );

        new OAuth2Interceptor(
                url, OAuth2Interceptor.GrantType.CLIENT_CREDENTIAL,
                "client", "secret",
                "user", null
        );

        new OAuth2Interceptor(
                url, OAuth2Interceptor.GrantType.CLIENT_CREDENTIAL,
                "client", "secret",
                "user", "password"
        );

        assertThatThrownBy(() -> {
            new OAuth2Interceptor(
                    url, OAuth2Interceptor.GrantType.CLIENT_CREDENTIAL,
                    "client", null,
                    "user", "password"
            );
        });

        assertThatThrownBy(() -> {
            new OAuth2Interceptor(
                    url, OAuth2Interceptor.GrantType.CLIENT_CREDENTIAL,
                    null, "secret",
                    "user", "password"
            );
        });

        assertThatThrownBy(() -> {
            new OAuth2Interceptor(
                    url, OAuth2Interceptor.GrantType.PASSWORD,
                    null, null,
                    null, "password"
            );
        });

        assertThatThrownBy(() -> {
            new OAuth2Interceptor(
                    url, OAuth2Interceptor.GrantType.PASSWORD,
                    null, null,
                    "user", null
            );
        });
    }

    @Test
    public void testClientCredential() throws IOException, InterruptedException {

        tokenServer.enqueue(new MockResponse().setBody("{\"access_token\":\"letoken\"}"));
        resourceServer.enqueue(new MockResponse().setBody("OK"));

        HttpUrl url = tokenServer.url("/token");
        OAuth2Interceptor interceptor = new OAuth2Interceptor(
                url, OAuth2Interceptor.GrantType.CLIENT_CREDENTIAL,
                "client", "secret",
                null, null
        );

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(interceptor).build();

        Request request = new Request.Builder()
                .url(resourceServer.url("/resource")).get().build();
        Response execute = client.newCall(request).execute();

        RecordedRequest tokenRequest = tokenServer.takeRequest();
        assertThat(tokenRequest.getBody().readByteString().utf8())
                .isEqualTo("" +
                        "client_id=client&" +
                        "client_secret=secret&" +
                        "grant_type=client_credentials&" +
                        "scope=openid%20profile%20email" +
                        "");

        RecordedRequest resourceRequest = resourceServer.takeRequest();
        assertThat(resourceRequest.getHeader("Authorization")).isEqualTo("Bearer letoken");
    }

    @Test
    public void testClientCredentialAndPassword() throws IOException, InterruptedException {

        tokenServer.enqueue(new MockResponse().setBody("{\"access_token\":\"letoken\"}"));
        resourceServer.enqueue(new MockResponse().setBody("OK"));

        HttpUrl url = tokenServer.url("/token");
        OAuth2Interceptor interceptor = new OAuth2Interceptor(
                url, OAuth2Interceptor.GrantType.CLIENT_CREDENTIAL,
                "client", "secret",
                "user", "password"
        );

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(interceptor).build();

        Request request = new Request.Builder()
                .url(resourceServer.url("/resource")).get().build();
        Response execute = client.newCall(request).execute();

        RecordedRequest tokenRequest = tokenServer.takeRequest();
        assertThat(tokenRequest.getBody().readByteString().utf8())
                .isEqualTo("" +
                        "client_id=client&" +
                        "client_secret=secret&" +
                        "username=user&" +
                        "password=password&" +
                        "grant_type=client_credentials&" +
                        "scope=openid%20profile%20email");

        RecordedRequest resourceRequest = resourceServer.takeRequest();
        assertThat(resourceRequest.getHeader("Authorization")).isEqualTo("Bearer letoken");
    }
}