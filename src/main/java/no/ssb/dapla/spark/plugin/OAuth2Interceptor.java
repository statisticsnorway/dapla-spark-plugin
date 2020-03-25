package no.ssb.dapla.spark.plugin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.dapla.spark.plugin.token.SparkConfStore;
import no.ssb.dapla.spark.plugin.token.TokenRefresher;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.spark.SparkConf;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * OAuth 2 interceptor that ensures that a user token exists in spark session.
 * The class also handles token renewals using the configured OAuth settings.
 */
public class OAuth2Interceptor implements Interceptor {


    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final TokenRefresher tokenRefresher;

    private static String extractClientSecret(String credentialsFile) {
        try {
            JsonNode credentialsJson = MAPPER.readTree(Paths.get(credentialsFile).toFile());
            return Objects.requireNonNull(credentialsJson.get("client_secret"),
                    String.format("Cannot find 'client_secret' in credentials file %s", credentialsFile)).asText();
        } catch (IOException e) {
            throw new RuntimeException("Error accessing credentials file: " + credentialsFile, e);
        }
    }

    private static String extractClientId(String credentialsFile) {
        try {
            JsonNode credentialsJson = MAPPER.readTree(Paths.get(credentialsFile).toFile());
            return Objects.requireNonNull(credentialsJson.get("client_id"),
                    String.format("Cannot find 'client_id' in credentials file %s", credentialsFile)).asText();
        } catch (IOException e) {
            throw new RuntimeException("Error accessing credentials file: " + credentialsFile, e);
        }
    }

    // Used in tests. This constructor skips token url validation.
    OAuth2Interceptor(HttpUrl tokenUrl, String clientId, String clientSecret, boolean ignoreExpiry, SparkConf conf) {
        // TODO: Move all the way down to the refresher.
        tokenRefresher = new TokenRefresher(tokenUrl);
        tokenRefresher.setTokenStore(new SparkConfStore(conf));
        tokenRefresher.setClientId(clientId);
        tokenRefresher.setClientSecret(clientSecret);
    }

    public OAuth2Interceptor(String tokenUrl, String credentialsFile, boolean ignoreExpiry, SparkConf conf) {
        this(tokenUrl, extractClientId(credentialsFile), extractClientSecret(credentialsFile), ignoreExpiry, conf);
    }

    public OAuth2Interceptor(String tokenUrl, String clientId, String clientSecret, boolean ignoreExpiry, SparkConf conf) {
        this(validateTokenUrl(tokenUrl), clientId, clientSecret, ignoreExpiry, conf);
    }

    private static HttpUrl validateTokenUrl(String tokenUrl) {
        return HttpUrl.get(Objects.requireNonNull(tokenUrl, "token url is required"));
    }

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        Request.Builder newRequest = chain.request().newBuilder();
        newRequest.header("Authorization", String.format("Bearer %s", tokenRefresher.getAccessToken()));
        return chain.proceed(newRequest.build());
    }
}