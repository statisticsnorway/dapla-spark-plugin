package no.ssb.dapla.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;

public class SparkServiceClient {

    static final ObjectMapper MAPPER = new ObjectMapper();
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

    public Dataset getDataset(String userId, String namespace) {
        Request request = new Request.Builder()
                .url(String.format(this.baseURL + "dataset-meta?name=%s&operation=READ&userId=%s", namespace, userId))
                .build();
        try {
            Response response = client.newCall(request).execute();
            if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {

            }
            System.out.println(response);
            String json = response.body().string();
            System.out.println(json);
            return ProtobufJsonUtils.toPojo(json, Dataset.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
