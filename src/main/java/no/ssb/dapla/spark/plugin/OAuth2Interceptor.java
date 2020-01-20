package no.ssb.dapla.spark.plugin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.util.Objects;

/**
 * OAuth 2 interceptor that gets a token.
 */
public class OAuth2Interceptor implements Interceptor {

    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final HttpUrl tokenUrl;
    private final String grantType;
    private final String clientId;
    private final String clientSecret;
    private String token = null;

    // Used in tests. This constructor skips token url validation.
    OAuth2Interceptor(HttpUrl tokenUrl, GrantType type, String clientId, String clientSecret) {
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.clientSecret = clientSecret;

        switch (Objects.requireNonNull(type)) {
            case CLIENT_CREDENTIAL:
                grantType = "client_credentials";
                Objects.requireNonNull(clientId, "client id is required");
                Objects.requireNonNull(clientSecret, "client secret is required");
                break;
            default:
                throw new IllegalArgumentException("Unsupported grant type " + type);
        }
    }

    public OAuth2Interceptor(String tokenUrl, GrantType type, String clientId, String clientSecret) {
        this(validateTokenUrl(tokenUrl), type, clientId, clientSecret);
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
        if (token == null) {
            token = fetchToken();
        }
        Request.Builder newRequest = chain.request().newBuilder();
        newRequest.header("Authorization", String.format("Bearer %s", token));
        return chain.proceed(newRequest.build());
    }

    private String fetchToken() throws IOException {
        FormBody.Builder formBodyBuilder = new FormBody.Builder();

        if (clientId != null) formBodyBuilder.add(CLIENT_ID, clientId);
        if (clientSecret != null) formBodyBuilder.add(CLIENT_SECRET, clientSecret);

        FormBody formBody = formBodyBuilder.add(GRANT_TYPE, grantType)
                .add("scope", "openid profile email")
                .build();

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

    public enum GrantType {
        CLIENT_CREDENTIAL
    }
}