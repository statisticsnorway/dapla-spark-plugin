package no.ssb.gsim.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.util.Objects;

/**
 * OAuth 2 interceptor that gets a token.
 */
public class OAuth2Interceptor implements Interceptor {

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String GRANT_TYPE = "grant_type";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String grantType;
    private final HttpUrl tokenUrl;
    private final String clientId;
    private final String clientSecret;
    private final String userName;
    private final String password;
    private String token = null;

    public OAuth2Interceptor(String tokenUrl, GrantType type, String clientId, String clientSecret, String userName,
                             String password) {
        this.tokenUrl = HttpUrl.get(Objects.requireNonNull(tokenUrl, "token url is required"));
        if (!this.tokenUrl.isHttps()) {
            throw new IllegalArgumentException("token url must be https");
        }
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.userName = userName;
        this.password = password;

        switch (Objects.requireNonNull(type)) {
            case PASSWORD:
                grantType = "password";
                Objects.requireNonNull(userName, "username is required");
                Objects.requireNonNull(password, "password is required");
                break;
            case CLIENT_CREDENTIAL:
                grantType = "client_credential";
                Objects.requireNonNull(clientId, "client id is required");
                Objects.requireNonNull(clientSecret, "client secret is required");
                break;
            default:
                throw new IllegalArgumentException("Unknown grant type " + type);
        }
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
        if (userName != null) formBodyBuilder.add(USERNAME, userName);
        if (password != null) formBodyBuilder.add(PASSWORD, password);
        if (clientId != null) formBodyBuilder.add(CLIENT_ID, clientId);

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
        PASSWORD,
        CLIENT_CREDENTIAL
    }
}