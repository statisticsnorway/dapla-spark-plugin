package no.ssb.dapla.service;

import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;

public class CatalogClient {

    public static final String CONFIG_CATALOG_URL = "spark.ssb.dapla.catalog.url";

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private OkHttpClient client;
    private String baseURL;

    public CatalogClient(final SparkConf conf) {
        init(conf);
    }

    public void init(final SparkConf conf) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        OAuth2Interceptor.createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        this.client = builder.build();
        this.baseURL = conf.get(CONFIG_CATALOG_URL);
        if (!this.baseURL.endsWith("/")) {
            this.baseURL = this.baseURL + "/";
        }
    }

    private String buildUrl(String format, Object... args) {
        return this.baseURL + String.format(format, args);
    }

    public ListByPrefixResponse listByPrefix(ListByPrefixRequest listByPrefixRequest) {
        Request request = new Request.Builder()
                .url(buildUrl("rpc/CatalogService/listByPrefix"))
                .post(RequestBody.create(ProtobufJsonUtils.toString(listByPrefixRequest), okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();

        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(response, json);
            ListByPrefixResponse readLocationResponse = ProtobufJsonUtils.toPojo(json, ListByPrefixResponse.class);
            return readLocationResponse;
        } catch (IOException e) {
            log.error("listByPrefix failed", e);
            throw new CatalogServiceException(e);
        }
    }


    private String getJson(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) return null;
        return body.string();
    }

    private void handleErrorCodes(Response response, String body) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new CatalogServiceException("Din bruker har ikke tilgang", body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new NotFoundException("Fant ingen datasett", body);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new CatalogServiceException("En feil har oppstått: " + response.toString(), body);
        }
    }

    public static class CatalogServiceException extends RuntimeException {
        private final String body;

        public CatalogServiceException(Throwable cause) {
            super(cause);
            this.body = null;
        }

        public CatalogServiceException(String message, String body) {
            super(message);
            this.body = body;
        }

        @Override
        public String getMessage() {
            if (body == null) {
                return super.getMessage();
            }
            return super.getMessage() + "\n" + body;
        }
    }

    public static class NotFoundException extends CatalogServiceException {
        public NotFoundException(String message, String body) {
            super(message, body);
        }
    }

}
