package no.ssb.dapla.service;

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
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Optional;

public class DataAccessClient {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    static final String CONFIG = "spark.ssb.dapla.";
    static final String CONFIG_ROUTER_URL = CONFIG + "router.url";
    static final String CONFIG_ROUTER_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    static final String CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE = CONFIG + "oauth.credentials.file";
    static final String CONFIG_ROUTER_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    static final String CONFIG_ROUTER_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";

    private OkHttpClient client;
    private String baseURL;

    public DataAccessClient(final SparkConf conf) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        this.client = builder.build();
        this.baseURL = conf.get(CONFIG_ROUTER_URL);
    }

    private Optional<OAuth2Interceptor> createOAuth2Interceptor(final SparkConf conf) {
        if (conf.contains(CONFIG_ROUTER_OAUTH_TOKEN_URL)) {
            OAuth2Interceptor interceptor = new OAuth2Interceptor(
                    conf.get(CONFIG_ROUTER_OAUTH_TOKEN_URL, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_ID, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, null)
            );
            return Optional.of(interceptor);
        }
        return Optional.empty();
    }

    private String buildUrl(String format, Object... args) {
        return this.baseURL + String.format(format, args);
    }

    public AccessTokenResponse getAccessToken(String userId, String path, AccessTokenRequest.Privilege privilege) {
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
            handleErrorCodes(userId, path, response, json);
            return ProtobufJsonUtils.toPojo(json, AccessTokenResponse.class);
        } catch (IOException e) {
            log.error("getAccessToken failed", e);
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
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

        public DataAccessServiceException(Throwable cause, String body) {
            super(cause);
            this.body = body;
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
