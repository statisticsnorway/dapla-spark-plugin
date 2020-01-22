package no.ssb.dapla.service;

import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.*;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;

public class SparkServiceClient {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    static final String CONFIG = "spark.ssb.dapla.";
    static final String CONFIG_ROUTER_URL = CONFIG + "router.url";
    static final String CONFIG_ROUTER_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    static final String CONFIG_ROUTER_OAUTH_GRANT_TYPE = CONFIG + "oauth.grantType";
    static final String CONFIG_ROUTER_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    static final String CONFIG_ROUTER_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";

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
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, null)
            );
            return Optional.of(interceptor);
        }
        return Optional.empty();
    }

    public void listNamespace(String namespace) {
        Request request = new Request.Builder()
                .url(buildUrl("prefix/%s", namespace))
                .build();
        try {
            Response response = client.newCall(request).execute();
            String json = response.body().string();
            System.out.println(json);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("listNamespace failed", e);
            throw e;
        }
    }

    private String buildUrl(String format, Object... args) {
        return this.baseURL + String.format(format, args);
    }

    public Dataset getDataset(String userId, String namespace) {
        Request request = new Request.Builder()
                .url(buildUrl("dataset-meta?name=%s&operation=READ&userId=%s", namespace, userId))
                .build();
        try {
            Response response = client.newCall(request).execute();
            handleErrorCodes(userId, namespace, response);
            String json = response.body().string();
            return ProtobufJsonUtils.toPojo(json, Dataset.class);
        } catch (IOException e) {
            log.error("getDataset failed", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("getDataset failed", e);
            throw e;
        }
    }

    public Dataset createDataset(String userId, SaveMode mode, String namespace, String valuation, String state) {
        String operation;
        if (mode == SaveMode.Append) {
            operation = "UPDATE";
        } else {
            operation = "CREATE"; // TODO: Check if this is correct for Overwrite
        }
        final String url = buildUrl("dataset-meta?name=%s&operation=%s&valuation=%s&state=%s&userId=%s",
                namespace, operation, valuation, state, userId);
        log.info("createDataset URL: {}", url);
        System.out.println("URL: " + url);
        Request request = new Request.Builder()
                .url(url)
                .build();
        try {
            Response response = client.newCall(request).execute();
            handleErrorCodes(userId, namespace, response);
            String json = response.body().string();
            return ProtobufJsonUtils.toPojo(json, Dataset.class);
        } catch (IOException e) {
            log.error("createDataset failed", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("createDataset failed", e);
            throw e;
        }
    }

    public void writeDataset(Dataset dataset) {
        String body = ProtobufJsonUtils.toString(dataset);
        Request request = new Request.Builder()
                .url(this.baseURL + "dataset-meta")
                .put(RequestBody.create(body, okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();
        try {
            Response response = client.newCall(request).execute();
            handleErrorCodes("userId", "namespace", response, body);
        } catch (IOException e) {
            log.error("writeDataset failed", e);
            throw new SparkServiceException(e, body);
        } catch (Exception e) {
            log.error("writeDataset failed", e);
            throw e;
        }
    }

    private void handleErrorCodes(String userId, String namespace, Response response) {
        handleErrorCodes(userId, namespace, response, null);
    }

    private void handleErrorCodes(String userId, String namespace, Response response, String body) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new SparkServiceException(String.format("Din bruker %s har ikke tilgang til %s", userId, namespace), body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new SparkServiceException(String.format("Fant ingen datasett for %s", namespace), body);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new SparkServiceException("En feil har oppstått: " + response.toString(), body);
        }
    }

    static class SparkServiceException extends RuntimeException {
        private final String body;

        public SparkServiceException(Throwable cause, String body) {
            super(cause);
            this.body = body;
        }

        public SparkServiceException(String message, String body) {
            super(message);
            this.body = body;
        }

        @Override
        public String toString() {
            if (body == null) {
                return super.toString();
            }
            return super.toString() + body;
        }
    }
}
