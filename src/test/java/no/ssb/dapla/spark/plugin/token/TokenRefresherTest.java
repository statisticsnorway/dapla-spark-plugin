package no.ssb.dapla.spark.plugin.token;


import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.sql.Date;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TokenRefresherTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private MockWebServer server;
    private HttpUrl keycloakUrl;

    @Before
    public void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        keycloakUrl = server.url("/keycloak");
    }

    @Test
    public void testSettingClientCredentials() throws InterruptedException, JsonProcessingException {
        SparkConfStore store = new SparkConfStore(new SparkConf());
        TokenRefresher refresher = new TokenRefresher(keycloakUrl);

        refresher.setClientId("client-id");
        refresher.setClientSecret("client-secret");

        // Fake access token
        String accessToken = JWT.create()
                .withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.SECONDS)))
                .sign(Algorithm.HMAC256("secret"));

        // Fake refresh token
        String refreshToken = JWT.create()
                .withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.SECONDS)))
                .sign(Algorithm.HMAC256("secret"));

        // Set the first token.
        store.putAccessToken(accessToken);
        store.putRefreshToken(refreshToken);

        refresher.setTokenStore(store);

        ObjectNode response = mapper.createObjectNode();
        response.put("access_token", "access-token");
        response.put("refresh_token", "refresh-token");

        server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(response)));

        // Wait for the token to refresh.
        Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));

        RecordedRequest recordedRequest = server.takeRequest();
        String body = recordedRequest.getBody().readString(Charset.defaultCharset());
        assertThat(body).contains(
                "client_id=client-id",
                "client_secret=client-secret",
                "grant_type=refresh_token"
        );

    }

    @Test
    public void testExceptionIsPropagated() throws InterruptedException, JsonProcessingException {
        SparkConfStore store = new SparkConfStore(new SparkConf());
        TokenRefresher refresher = new TokenRefresher(keycloakUrl);

        // Fake access token
        String accessToken = JWT.create()
                .withExpiresAt(Date.from(Instant.now().plus(5, ChronoUnit.SECONDS)))
                .sign(Algorithm.HMAC256("secret"));

        // Fake refresh token
        String refreshToken = JWT.create()
                .withExpiresAt(Date.from(Instant.now().plus(5, ChronoUnit.SECONDS)))
                .sign(Algorithm.HMAC256("secret"));

        // Set the first token.
        store.putAccessToken(accessToken);
        store.putRefreshToken(refreshToken);

        server.enqueue(new MockResponse().setResponseCode(500));
        refresher.setTokenStore(store);

        // Wait for the token to refresh.
        // TODO: Rewrite, this will fail sometimes.
        Thread.sleep(TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));

        assertThatThrownBy(refresher::getAccessToken);

    }

    @Test
    public void testRefreshHappens() throws InterruptedException, JsonProcessingException {
        SparkConfStore store = new SparkConfStore(new SparkConf());

        Instant now = Instant.now().plus(2, ChronoUnit.SECONDS);

        // Fake access token
        String accessToken = JWT.create()
                .withExpiresAt(Date.from(now))
                .sign(Algorithm.HMAC256("secret"));

        // Fake refresh token
        String refreshToken = JWT.create()
                .withExpiresAt(Date.from(now))
                .sign(Algorithm.HMAC256("secret"));

        // Set the first token.
        store.putAccessToken(accessToken);
        store.putRefreshToken(refreshToken);

        now = now.plus(2, ChronoUnit.SECONDS);
        // Fake access token
        String newAccessToken = JWT.create()
                .withExpiresAt(Date.from(now))
                .sign(Algorithm.HMAC256("secret"));

        // Fake refresh token
        String newRefreshToken = JWT.create()
                .withExpiresAt(Date.from(now))
                .sign(Algorithm.HMAC256("secret"));

        ObjectNode response = mapper.createObjectNode();
        response.put("access_token", newAccessToken);
        response.put("refresh_token", newRefreshToken);

        server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(response)));


        TokenRefresher refresher = new TokenRefresher(keycloakUrl);
        refresher.setTokenStore(store);

        // Wait for the token to refresh.
        Thread.sleep(TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS));

        assertThat(refresher.getAccessToken()).isEqualTo(newAccessToken);
        assertThat(store.getAccessToken()).isEqualTo(newAccessToken);
        assertThat(store.getRefreshToken()).isEqualTo(newRefreshToken);

    }
}