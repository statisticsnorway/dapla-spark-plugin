package no.ssb.dapla.spark.plugin.token;

import com.auth0.jwt.JWT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Utility class to ensure token renewal.
 */
public class TokenRefresher implements Runnable, Supplier<String> {

    private static final Logger log = LoggerFactory.getLogger(TokenRefresher.class);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> nextUpdate;
    private TokenStore tokenStore;

    public void setTokenStore(TokenStore tokenStore) {
        this.tokenStore = Objects.requireNonNull(tokenStore);
        scheduleNextRefresh();
    }

    private void scheduleNextRefresh() {
        if (!nextUpdate.isDone()) {
            log.debug("Refresh already scheduled");
            return;
        }
        String token = tokenStore.getAccessToken();
        Instant expiresAt = JWT.decode(token).getExpiresAt().toInstant();
        Duration timeBeforeExpiration = Duration.between(Instant.now(), expiresAt);
        log.info("scheduling token refresh in {}", timeBeforeExpiration);
        nextUpdate = scheduler.schedule(this, timeBeforeExpiration.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void run() {

    }

    @Override
    public String get() {

        return null;
    }
}
