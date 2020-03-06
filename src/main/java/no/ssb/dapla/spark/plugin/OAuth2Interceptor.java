package no.ssb.dapla.spark.plugin;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_TOKEN_URL;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_CLIENT_ID;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_CLIENT_SECRET;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.SPARK_SSB_ACCESS_TOKEN;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.SPARK_SSB_RENEW_TOKEN;

/**
 * OAuth 2 interceptor that ensures that a user token exists in spark session.
 * The class also handles token renewals using the configured OAuth settings.
 */
public class OAuth2Interceptor implements Interceptor {

    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final String REFRESH_TOKEN = "refresh_token";

    private static final String DEFAULT_GRANT_TYPE = "client_credentials";
    private static final String GRANT_TYPE_REFRESH = "refresh_token";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final HttpUrl tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final SparkConf conf;

    public static Optional<OAuth2Interceptor> createOAuth2Interceptor(final SparkConf conf) {
        if (conf.contains(CONFIG_ROUTER_OAUTH_TOKEN_URL)) {
            OAuth2Interceptor interceptor = new OAuth2Interceptor(
                    conf.get(CONFIG_ROUTER_OAUTH_TOKEN_URL, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_ID, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, null),
                    conf
            );
            return Optional.of(interceptor);
        }
        return Optional.empty();
    }

    // Used in tests. This constructor skips token url validation.
    OAuth2Interceptor(HttpUrl tokenUrl, String credentialsFile, String clientId, String clientSecret, SparkConf conf) {
        this.tokenUrl = tokenUrl;
        this.conf = conf;
        try {
            if (credentialsFile != null) {
                JsonNode credentialsJson = MAPPER.readTree(Paths.get(credentialsFile).toFile());
                this.clientId = Objects.requireNonNull(credentialsJson.get("client_id"),
                        String.format("Cannot find 'client_id' in credentials file %s", credentialsFile)).asText();
                this.clientSecret = Objects.requireNonNull(credentialsJson.get("client_secret"),
                        String.format("Cannot find 'client_secret' in credentials file %s", credentialsFile)).asText();
            } else {
                this.clientId = Objects.requireNonNull(clientId, "client id is required");
                this.clientSecret = Objects.requireNonNull(clientSecret, "client secret is required");
            }
        } catch (IOException e) {
            throw new RuntimeException("Error accessing credentials file: " + credentialsFile, e);
        }
    }

    public OAuth2Interceptor(String tokenUrl, String credentialsFile, String clientId, String clientSecret, SparkConf conf) {
        this(validateTokenUrl(tokenUrl), credentialsFile, clientId, clientSecret, conf);
    }

    private static HttpUrl validateTokenUrl(String tokenUrl) {
        HttpUrl tokenHttpUrl = HttpUrl.get(Objects.requireNonNull(tokenUrl, "token url is required"));
        if (!tokenHttpUrl.isHttps()) {
            throw new IllegalArgumentException("token url must be https");
        }
        return tokenHttpUrl;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        String token = getToken();
        if (token == null) {
            token = fetchToken(null);
            setToken(token);
        } else if (tokenExpired(token)) {
            token = fetchToken(getRenewToken());
            setToken(token);
        }
        Request.Builder newRequest = chain.request().newBuilder();
        newRequest.header("Authorization", String.format("Bearer %s", token));
        return chain.proceed(newRequest.build());
    }

    private String getToken() {
        return conf.get(SPARK_SSB_ACCESS_TOKEN);
    }

    private String getRenewToken() {
        return conf.get(SPARK_SSB_RENEW_TOKEN);
    }

    private void setToken(String token) {
        conf.set(SPARK_SSB_ACCESS_TOKEN, token);
    }

    private boolean tokenExpired(String token) {
        DecodedJWT decodedJWT = JWT.decode(token);
        return new Date().after(decodedJWT.getExpiresAt());
    }

    private String fetchToken(final String renewToken) throws IOException {
        FormBody.Builder formBodyBuilder = new FormBody.Builder();

        if (clientId != null) formBodyBuilder.add(CLIENT_ID, clientId);
        if (clientSecret != null) formBodyBuilder.add(CLIENT_SECRET, clientSecret);
        if (clientSecret != null) formBodyBuilder.add(CLIENT_SECRET, clientSecret);

        if (renewToken != null) {
            formBodyBuilder.add(REFRESH_TOKEN, renewToken);
            formBodyBuilder.add(GRANT_TYPE, GRANT_TYPE_REFRESH);
        } else {
            formBodyBuilder.add(GRANT_TYPE, DEFAULT_GRANT_TYPE);
        }

        FormBody formBody = formBodyBuilder.add("scope", "openid profile email").build();

        Request request = new Request.Builder()
                .url(tokenUrl)
                .post(formBody)
                .build();

        OkHttpClient client = new OkHttpClient();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("authentication failed" + response);
            }
            ResponseBody body = response.body();
            if (body == null) {
                throw new IOException("empty response");
            }
            JsonNode bodyContent = MAPPER.readTree(body.bytes());
            return bodyContent.get("access_token").asText();
        }

    }
}