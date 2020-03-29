package no.ssb.dapla.spark.plugin.token;

import com.auth0.jwt.JWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Class that ensures token refresh.
 */
public class TokenRefresher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(TokenRefresher.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Integer REFRESH_RUSH_SECONDS = 10;

    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final String REFRESH_TOKEN = "refresh_token";

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> nextUpdate;

    private final HttpUrl tokenUrl;
    private String clientId;
    private String clientSecret;
    private TokenStore tokenStore;
    private Exception exception;

    public TokenRefresher(HttpUrl tokenUrl) {
        this.tokenUrl = Objects.requireNonNull(tokenUrl);
    }

    public void setTokenStore(TokenStore tokenStore) {
        this.tokenStore = Objects.requireNonNull(tokenStore);
        scheduleNextRefresh();
    }

    @Override
    public void run() {
        try {
            refreshToken();
        } catch (Exception e) {
            exception = e;
            log.error("Could not refresh token", e);
        } finally {
            scheduleNextRefresh();
        }
    }

    private void scheduleNextRefresh() {
        if (nextUpdate != null && !nextUpdate.isDone()) {
            log.debug("Refresh already scheduled");
            return;
        }
        String token = tokenStore.getAccessToken();
        Instant expiresAt = JWT.decode(token).getExpiresAt().toInstant();
        Duration timeBeforeExpiration = Duration.between(Instant.now(), expiresAt);

        if (timeBeforeExpiration.isNegative()) {
            throw new IllegalArgumentException("expiration was in the past: " + expiresAt);
        }

        // Account for network delays etc.
        if (!timeBeforeExpiration.minus(REFRESH_RUSH_SECONDS, ChronoUnit.SECONDS).isNegative()) {
            timeBeforeExpiration = timeBeforeExpiration.minus(REFRESH_RUSH_SECONDS, ChronoUnit.SECONDS);
        }

        log.info("Scheduling token refresh in {}", timeBeforeExpiration);
        nextUpdate = scheduler.schedule(this, timeBeforeExpiration.getSeconds(), TimeUnit.SECONDS);
    }

    /**
     * Run by the executor.
     */
    private void refreshToken() throws IOException {
        FormBody.Builder formBodyBuilder = new FormBody.Builder();

        if (clientId != null) {
            formBodyBuilder.add(CLIENT_ID, clientId);
        }
        if (clientSecret != null) {
            formBodyBuilder.add(CLIENT_SECRET, clientSecret);
        }

        formBodyBuilder.add(GRANT_TYPE, REFRESH_TOKEN);
        formBodyBuilder.add(REFRESH_TOKEN, tokenStore.getRefreshToken());

        FormBody formBody = formBodyBuilder.build();

        Request request = new Request.Builder()
                .url(tokenUrl)
                .post(formBody)
                .build();

        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("authentication failed " + response);
            }
            ResponseBody body = response.body();
            if (body == null) {
                throw new IOException("empty response");
            }
            JsonNode bodyContent = MAPPER.readTree(body.bytes());

            tokenStore.putAccessToken(bodyContent.get("access_token").asText());
            tokenStore.putRefreshToken(bodyContent.get("refresh_token").asText());

        }
    }

    public String getAccessToken() {
        if (exception != null) {
            Exception toThrow = exception;
            exception = null;
            if (toThrow instanceof RuntimeException) {
                throw (RuntimeException) toThrow;
            } else {
                throw new RuntimeException(toThrow);
            }
        }
        return tokenStore.getAccessToken();
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }
}
