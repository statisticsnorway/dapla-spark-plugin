package no.ssb.dapla.service;

import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
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

public class DataAccessClient {

    public static final String CONFIG_DATA_ACCESS_URL = "spark.ssb.dapla.data.access.url";

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private OkHttpClient client;
    private String baseURL;

    public DataAccessClient(final SparkConf conf) {
        init(conf);
    }

    public void init(final SparkConf conf) {
        okhttp3.OkHttpClient.Builder builder = new okhttp3.OkHttpClient.Builder();
        OAuth2Interceptor.createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        this.client = builder.build();
        this.baseURL = conf.get(CONFIG_DATA_ACCESS_URL);
        if (!this.baseURL.endsWith("/")) {
            this.baseURL = this.baseURL + "/";
        }
    }

    private String buildUrl(String format, Object... args) {
        return this.baseURL + String.format(format, args);
    }

    public ReadLocationResponse readLocation(ReadLocationRequest readLocationRequest) {
        Request request = new Request.Builder()
                .url(buildUrl("rpc/DataAccessService/readLocation"))
                .post(RequestBody.create(ProtobufJsonUtils.toString(readLocationRequest), okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();

        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(response, json);
            ReadLocationResponse readLocationResponse = ProtobufJsonUtils.toPojo(json, ReadLocationResponse.class);
            return readLocationResponse;
        } catch (IOException e) {
            log.error("readLocation failed", e);
            throw new DataAccessServiceException(e);
        }
    }

    public ReadAccessTokenResponse readAccessToken(ReadAccessTokenRequest readAccessTokenRequest) {
        Request request = new Request.Builder()
                .url(buildUrl("rpc/DataAccessService/readAccessToken"))
                .post(RequestBody.create(ProtobufJsonUtils.toString(readAccessTokenRequest), okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();

        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(response, json);
            ReadAccessTokenResponse readAccessTokenResponse = ProtobufJsonUtils.toPojo(json, ReadAccessTokenResponse.class);
            return readAccessTokenResponse;
        } catch (IOException e) {
            log.error("readAccessToken failed", e);
            throw new DataAccessServiceException(e);
        }
    }

    public WriteLocationResponse writeLocation(WriteLocationRequest writeLocationRequest) {
        Request request = new Request.Builder()
                .url(buildUrl("rpc/DataAccessService/writeLocation"))
                .post(RequestBody.create(ProtobufJsonUtils.toString(writeLocationRequest), okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();

        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(response, json);
            WriteLocationResponse writeLocationResponse = ProtobufJsonUtils.toPojo(json, WriteLocationResponse.class);
            return writeLocationResponse;
        } catch (IOException e) {
            log.error("writeLocation failed", e);
            throw new DataAccessServiceException(e);
        }
    }

    public WriteAccessTokenResponse writeAccessToken(WriteAccessTokenRequest writeAccessTokenRequest) {
        Request request = new Request.Builder()
                .url(buildUrl("rpc/DataAccessService/writeAccessToken"))
                .post(RequestBody.create(ProtobufJsonUtils.toString(writeAccessTokenRequest), okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();

        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(response, json);
            WriteAccessTokenResponse writeAccessTokenResponse = ProtobufJsonUtils.toPojo(json, WriteAccessTokenResponse.class);
            return writeAccessTokenResponse;
        } catch (IOException e) {
            log.error("writeAccessToken failed", e);
            throw new DataAccessServiceException(e);
        }
    }

    private String getJson(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) return null;
        return body.string();
    }

    private void handleErrorCodes(Response response, String body) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new DataAccessServiceException("Din bruker har ikke tilgang", body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new DataAccessServiceException("Fant ikke datasett", body);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new DataAccessServiceException("En feil har oppstått: " + response.toString(), body);
        }
    }

    public static class DataAccessServiceException extends RuntimeException {
        private final String body;

        public DataAccessServiceException(Throwable cause) {
            super(cause);
            this.body = null;
        }

        public DataAccessServiceException(String message, String body) {
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
}
