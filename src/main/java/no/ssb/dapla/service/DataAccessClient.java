package no.ssb.dapla.service;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.LocationRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;

public class DataAccessClient {

    public static final String CONFIG_DATA_ACCESS_URL = "spark.ssb.dapla.data.access.url";

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private OkHttpClient client;
    private String baseURL;

    public DataAccessClient(final Configuration conf) {
        init(getSparkConf(conf));
    }

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

    private SparkConf getSparkConf(Configuration conf) {
        SparkConf sparkConf = new SparkConf();
        for (Map.Entry<String,String> entry: conf) {
            if (entry.getKey().startsWith("spark.")) {
                sparkConf.set(entry.getKey(), entry.getValue());
            }
        }
        return sparkConf;
    }

    private String buildUrl(String format, Object... args) {
        return this.baseURL + String.format(format, args);
    }

    public AccessTokenProvider.AccessToken getAccessToken(String userId, String path, AccessTokenRequest.Privilege privilege) {
        AccessTokenRequest tokenRequest = AccessTokenRequest.newBuilder()
                .setUserId(userId)
                .setPath(path)
                .setPrivilege(privilege)
                .build();

        String body = ProtobufJsonUtils.toString(tokenRequest);

        Request request = new Request.Builder()
                .url(buildUrl("rpc/DataAccessService/getAccessToken"))
                .post(RequestBody.create(body, okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();
        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(userId, path, privilege, response, json);
            return toAccessToken(json);
        } catch (IOException e) {
            log.error("getAccessToken failed", e);
            throw new DataAccessServiceException(e);
        } catch (Exception e) {
            log.error("getAccessToken failed", e);
            throw e;
        }
    }

    public LocationResponse getLocationWithLatestVersion(String userId, String path) {
        return getLocation(userId, path, 0);
    }

    public LocationResponse getLocation(String userId, String path, long snapshot) {
        LocationRequest locationRequest = LocationRequest.newBuilder()
                .setUserId(userId)
                .setSnapshot(snapshot)
                .setPath(path)
                .build();

        String body = ProtobufJsonUtils.toString(locationRequest);

        Request request = new Request.Builder()
                .url(buildUrl("rpc/DataAccessService/getLocation"))
                .post(RequestBody.create(body, okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();
        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(userId, path, response, json);
            return ProtobufJsonUtils.toPojo(json, LocationResponse.class);
        } catch (IOException e) {
            log.error("getLocation failed", e);
            throw new DataAccessServiceException(e);
        } catch (Exception e) {
            log.error("getLocation failed", e);
            throw e;
        }
    }

    private String getJson(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) return null;
        return body.string();
    }

    private AccessTokenProvider.AccessToken toAccessToken(String json) {
        AccessTokenResponse response = ProtobufJsonUtils.toPojo(json, AccessTokenResponse.class);
        return new AccessTokenProvider.AccessToken(response.getAccessToken(), response.getExpirationTime());
    }


    private void handleErrorCodes(String userId, String location, AccessTokenRequest.Privilege privilege,
                                  Response response, String body) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new DataAccessServiceException(String.format("Din bruker %s har ikke %s tilgang til %s",
                    userId, privilege.name(), location), body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new DataAccessServiceException(String.format("Fant ingen location %s", location), body);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new DataAccessServiceException("En feil har oppstått: " + response.toString(), body);
        }
    }

    private void handleErrorCodes(String userId, String namespace, Response response, String body) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new DataAccessServiceException(String.format("Din bruker %s har ikke tilgang til %s", userId, namespace), body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new DataAccessServiceException(String.format("Fant ingen datasett for %s", namespace), body);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new DataAccessServiceException("En feil har oppstått: " + response.toString(), body);
        }
    }

    static class DataAccessServiceException extends RuntimeException {
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
