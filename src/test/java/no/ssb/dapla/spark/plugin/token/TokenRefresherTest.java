package no.ssb.dapla.spark.plugin.token;


import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.charset.Charset;
import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Ignore("This test is too fragile")
public class TokenRefresherTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private MockWebServer server;
    private TokenGeneratorStore store;

    @Before
    public void setUp() throws Exception {
        store = new TokenGeneratorStore(new SparkConf());
        server = new MockWebServer();
        server.start();
        store.setTokenUrl(server.url("/keycloak"));
    }

    @Test
    public void testSettingClientCredentials() throws InterruptedException, JsonProcessingException {

        ObjectNode response = mapper.createObjectNode();
        response.put("access_token", "access-token");
        response.put("refresh_token", "refresh-token");

        server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(response)));

        store.setDelay(Duration.of(1, SECONDS));
        store.setClientId("client-id");
        store.setClientSecret("client-secret");

        new TokenRefresher(store);

        // Wait for the token to refresh.
        store.awaitAccessToken();
        store.awaitRefreshToken();

        RecordedRequest recordedRequest = server.takeRequest();
        String body = recordedRequest.getBody().readString(Charset.defaultCharset());
        assertThat(body).contains(
                "client_id=client-id",
                "client_secret=client-secret",
                "grant_type=refresh_token"
        );

    }

    @Test
    public void testExceptionIsPropagated() throws InterruptedException {
        server.enqueue(new MockResponse().setResponseCode(500));

        store.setDelay(Duration.of(1, SECONDS));
            TokenRefresher refresher = new TokenRefresher(store);

        server.takeRequest(3, TimeUnit.SECONDS);

        assertThatThrownBy(refresher::get);
    }

    @Test
    public void testRefreshHappens() throws InterruptedException, JsonProcessingException {

        ObjectNode response = mapper.createObjectNode();
        response.put("access_token", "newAccessToken");
        response.put("refresh_token", "newRefreshToken");

        server.enqueue(new MockResponse().setBody(mapper.writeValueAsString(response)));

        store.setDelay(Duration.of(1, SECONDS));
        TokenRefresher refresher = new TokenRefresher(store);

        // Wait for the token to refresh.
        store.awaitRefreshToken();

        RecordedRequest recordedRequest = server.takeRequest();
        String body = recordedRequest.getBody().readString(Charset.defaultCharset());

        assertThat(body).contains(
                "refresh_token=" + store.lastRefreshToken,
                "grant_type=refresh_token"
        );
        assertThat(refresher.get()).isEqualTo(store.lastAccessToken);
        assertThat(store.getAccessToken()).isEqualTo(store.lastAccessToken);
        assertThat(store.getRefreshToken()).isEqualTo(store.lastRefreshToken);

    }

    private static class TokenGeneratorStore extends SparkConfStore {

        private Duration delay;
        private Semaphore accessSemaphore = new Semaphore(0);
        private Semaphore refreshSemaphore = new Semaphore(0);
        private String lastAccessToken;
        private String lastRefreshToken;

        public TokenGeneratorStore(SparkConf conf) {
            super(conf);
        }

        public String awaitAccessToken() throws InterruptedException {
            accessSemaphore.acquire();
            return lastAccessToken;
        }

        public String awaitRefreshToken() throws InterruptedException {
            refreshSemaphore.acquire();
            return lastRefreshToken;
        }

        @Override
        public void putAccessToken(String access) {
            super.putAccessToken(access);
            accessSemaphore.release();
        }

        @Override
        public void putRefreshToken(String refresh) {
            super.putRefreshToken(refresh);
            refreshSemaphore.release();
        }

        @Override
        public String getAccessToken() {
            lastAccessToken = JWT.create().withExpiresAt(Date.from(Instant.now().plus(delay))).sign(Algorithm.HMAC256("secret"));
            return lastAccessToken;
        }

        @Override
        public String getRefreshToken() {
            lastRefreshToken = JWT.create().withExpiresAt(Date.from(Instant.now().plus(delay))).sign(Algorithm.HMAC256("secret"));
            return lastRefreshToken;
        }

        public void setDelay(Duration delay) {
            this.delay = delay;
        }
    }
}