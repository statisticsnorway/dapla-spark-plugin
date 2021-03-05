package no.ssb.dapla.spark.plugin.token;

import com.auth0.jwt.JWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;

public class CustomAuthSupplier implements TokenSupplier {

    private static final Integer TOKEN_EXPIRY_BUFFER_SECS = 10;
    private static final AtomicReference<String> TOKEN = new AtomicReference<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(CustomAuthSupplier.class);

    public CustomAuthSupplier(final SparkConf conf) {
        TOKEN.compareAndSet(null, conf.get(SPARK_SSB_ACCESS_TOKEN));
        if (shouldRefresh()) {
            refreshToken();
        }
    }

    private boolean shouldRefresh() {
        if (TOKEN.get() == null) return true;

        Instant expiresAt = JWT.decode(TOKEN.get()).getExpiresAt().toInstant();
        Duration timeBeforeExpiration = Duration.between(Instant.now(), expiresAt);

        // Account for network delays etc.
        return timeBeforeExpiration.minus(TOKEN_EXPIRY_BUFFER_SECS, ChronoUnit.SECONDS).isNegative();
    }

    private void refreshToken() {
        LOG.info("Refresh user token from " + System.getenv("JUPYTERHUB_HANDLER_CUSTOM_AUTH_URL"));
        try {
            Request request = new Request.Builder()
                    .url(System.getenv("JUPYTERHUB_HANDLER_CUSTOM_AUTH_URL"))
                    .addHeader("Authorization", String.format("token %s", System.getenv("JUPYTERHUB_API_TOKEN"))).get()
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
                TOKEN.set(bodyContent.get("access_token").asText("access_token"));
            }
        } catch (IOException ioe) {
            LOG.error("Error in refreshToken", ioe);
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public String get() {
        return TOKEN.get();
    }
}
