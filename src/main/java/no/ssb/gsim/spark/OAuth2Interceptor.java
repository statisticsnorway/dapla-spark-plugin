package no.ssb.gsim.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OAuth 2 interceptor that gets a token.
 */
public class OAuth2Interceptor implements Interceptor {

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";
    private static final String CLIENT_ID = "client_id";
    private static final String GRANT_TYPE = "grant_type";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String tokenUrl;
    private final String clientId;
    private final String userName;
    private final String password;
    private String token = null;

    public OAuth2Interceptor(String tokenUrl, String clientId, String userName, String password) {
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.userName = userName;
        this.password = password;
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
        RequestBody formBody = new FormBody.Builder()
                .add(CLIENT_ID, clientId)
                .add(USERNAME, userName)
                .add(PASSWORD, password)
                .add(GRANT_TYPE, "password")
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
}