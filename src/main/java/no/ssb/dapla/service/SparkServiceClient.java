package no.ssb.dapla.service;

import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;

public class SparkServiceClient {

    static final String CONFIG = "spark.ssb.dapla.";
    static final String CONFIG_ROUTER_URL = CONFIG + "router.url";
    static final String CONFIG_ROUTER_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    static final String CONFIG_ROUTER_OAUTH_GRANT_TYPE = CONFIG + "oauth.grantType";
    static final String CONFIG_ROUTER_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    static final String CONFIG_ROUTER_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";
    static final String CONFIG_ROUTER_OAUTH_USER_NAME = CONFIG + "oauth.userName";
    static final String CONFIG_ROUTER_OAUTH_PASSWORD = CONFIG + "oauth.password";

    private OkHttpClient client;
    private String baseURL;

    public SparkServiceClient(final SparkConf conf) {
        okhttp3.OkHttpClient.Builder builder = new okhttp3.OkHttpClient.Builder();
        createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        this.client = builder.build();
        this.baseURL = conf.get(CONFIG_ROUTER_URL);
    }

    private Optional<OAuth2Interceptor> createOAuth2Interceptor(final SparkConf conf) {
        if (conf.contains(CONFIG_ROUTER_OAUTH_TOKEN_URL)) {
            OAuth2Interceptor interceptor = new OAuth2Interceptor(
                    conf.get(CONFIG_ROUTER_OAUTH_TOKEN_URL, null),
                    OAuth2Interceptor.GrantType.valueOf(
                            conf.get(CONFIG_ROUTER_OAUTH_GRANT_TYPE).toUpperCase()
                    ),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_ID, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, null),
                    conf.get(CONFIG_ROUTER_OAUTH_USER_NAME, null),
                    conf.get(CONFIG_ROUTER_OAUTH_PASSWORD, null)
            );
            return Optional.of(interceptor);
        }
        return Optional.empty();
    }

    public void listNamespace(String namespace) {
        Request request = new Request.Builder()
                .url(String.format(this.baseURL + "prefix/%s", namespace))
                .build();
        try {
            Response response = client.newCall(request).execute();
            String json = response.body().string();
            System.out.println(json);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Dataset getDataset(String userId, String namespace) {
        Request request = new Request.Builder()
                .url(String.format(this.baseURL + "dataset-meta?name=%s&operation=READ&userId=%s", namespace, userId))
                .build();
        try {
            Response response = client.newCall(request).execute();
            handleErrorCodes(userId, namespace, response);
            String json = response.body().string();
            return ProtobufJsonUtils.toPojo(json, Dataset.class);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Dataset createDataset(String userId, SaveMode mode, String namespace, String valuation, String state) {
        String operation;
        switch (mode) {
            case Append:
                operation = "UPDATE";
                break;
            default:
                operation = "CREATE"; // TODO: Check if this is correct for Overwrite
        }
        Request request = new Request.Builder()
                .url(String.format(this.baseURL + "dataset-meta?name=%s&operation=%s&valuation=%s&state=%s&userId=%s",
                        namespace, operation, valuation, state, userId))
                .build();
        try {
            Response response = client.newCall(request).execute();
            handleErrorCodes(userId, namespace, response);
            String json = response.body().string();
            return ProtobufJsonUtils.toPojo(json, Dataset.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeDataset(Dataset dataset) {
        Request request = new Request.Builder()
                .url(this.baseURL + "dataset-meta")
                .put(RequestBody.create(ProtobufJsonUtils.toString(dataset), okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();
        try {
            Response response = client.newCall(request).execute();
            handleErrorCodes("userId", "namespace", response);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleErrorCodes(String userId, String namespace, Response response) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new RuntimeException(String.format("Din bruker %s har ikke tilgang til %s", userId, namespace));
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new RuntimeException(String.format("Fant ingen datasett for %s", namespace));
        } else if (response.code() < 200 && response.code() >= 400) {
            throw new RuntimeException("En feil har oppstått: " + response.toString());
        }
    }

}
