package no.ssb.dapla.spark.plugin;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OAuth2InterceptorTest {

    private MockWebServer tokenServer;
    private MockWebServer resourceServer;
    private File tmpDir;
    private Path credentialsFile;
    private final String credentialsJson = "{\n" +
            "  \"client_id\": \"myClient\",\n" +
            "  \"client_secret\": \"mySecret\"\n" +
            "}";

    @Before
    public void setUp() throws Exception {
        this.tokenServer = new MockWebServer();
        this.resourceServer = new MockWebServer();
        this.tokenServer.start();
        this.resourceServer.start();
        tmpDir = Files.createTempDirectory("OAuth2InterceptorTest").toFile();
        credentialsFile = tmpDir.toPath().resolve("creds.json");
        Files.write(credentialsFile , credentialsJson.getBytes());
    }

    @After
    public void tearDown() throws Exception {
        tokenServer.shutdown();
        resourceServer.shutdown();
        credentialsFile.toFile().delete();
        tmpDir.delete();
    }

    @Test
    public void testInvalidCombinations() {

        HttpUrl url = HttpUrl.get("http://localhost/");
        new OAuth2Interceptor(
                url, null,
                "client", "secret"
        );

        new OAuth2Interceptor(
                url, credentialsFile.toString(),
                null, null
        );

        assertThatThrownBy(() -> {
            new OAuth2Interceptor(
                    url, null,
                    "client", null
            );
        });

        assertThatThrownBy(() -> {
            new OAuth2Interceptor(
                    url, null,
                    null, "secret"
            );
        });

        assertThatThrownBy(() -> {
            new OAuth2Interceptor(
                    url, null,
                    null, null
            );
        });

    }

    @Test
    public void testClientCredential() throws IOException, InterruptedException {

        tokenServer.enqueue(new MockResponse().setBody("{\"access_token\":\"letoken\"}"));
        resourceServer.enqueue(new MockResponse().setBody("OK"));

        HttpUrl url = tokenServer.url("/token");
        OAuth2Interceptor interceptor = new OAuth2Interceptor(
                url, null,
                "client", "secret"
        );

        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(interceptor).build();

        Request request = new Request.Builder()
                .url(resourceServer.url("/resource")).get().build();
        client.newCall(request).execute();

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

}