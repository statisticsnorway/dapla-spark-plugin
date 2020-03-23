package no.ssb.dapla.spark.plugin.token;


import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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
    public void testRefreshHappens() throws InterruptedException, JsonProcessingException {
        SparkConfStore store = new SparkConfStore(new SparkConf());

        Instant now = Instant.now().plus(1, ChronoUnit.SECONDS);

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

        now = now.plus(1, ChronoUnit.SECONDS);
        // Fake access token
        String newAccessToken = JWT.create()
                .withExpiresAt(Date.from(now))
                .sign(Algorithm.HMAC256("secret"));

        // Fake refresh token
        String newRefreshToken = JWT.create()
                .withExpiresAt(Date.from(now.plus(1, ChronoUnit.HOURS)))
                .sign(Algorithm.HMAC256("secret"));

        ObjectNode response = mapper.createObjectNode();
        response.put("access_token", newAccessToken);
        response.put("refresh_token", newRefreshToken);

        server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(response)));


        TokenRefresher refresher = new TokenRefresher(keycloakUrl);
        refresher.setTokenStore(store);

        // Wait for the token to refresh.
        Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));

        assertThat(store.getAccessToken()).isEqualTo(newAccessToken);
        assertThat(store.getRefreshToken()).isEqualTo(newRefreshToken);

    }
}