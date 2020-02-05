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
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;

public class DataAccessClient {

    static final String CONFIG_DATA_ACCESS_URL = "spark.ssb.dapla.data.access.url";

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private OkHttpClient client;
    private String baseURL;

    public DataAccessClient(final SparkConf conf) {
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

    public AccessTokenProvider.AccessToken getAccessToken(String userId, String location,
                                                          AccessTokenRequest.Privilege privilege) {
        Request request = new Request.Builder()
                .url(buildUrl("access/token?userId=%s&location=%s&privilege=%s", userId, location, privilege.name()))
                .build();
        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(userId, location, privilege, response, json);
            return toAccessToken(json);
        } catch (IOException e) {
            log.error("getAccessToken failed", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("getAccessToken failed", e);
            throw e;
        }
    }

    public String getLocation(LocationRequest.Valuation valuation, LocationRequest.DatasetState state) {
        Request request = new Request.Builder()
                .url(buildUrl("access/location?valuation=%s&state=%s", valuation.name(), state.name()))
                .build();
        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(valuation, state, response, json);
            return ProtobufJsonUtils.toPojo(json, LocationResponse.class).getLocation();
        } catch (IOException e) {
            log.error("getAccessToken failed", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("getAccessToken failed", e);
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
            throw new SparkServiceClient.SparkServiceException(String.format("Din bruker %s har ikke %s tilgang til %s",
                    userId, privilege.name(), location), body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new SparkServiceClient.SparkServiceException(String.format("Fant ingen location %s", location), body);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new SparkServiceClient.SparkServiceException("En feil har oppstått: " + response.toString(), body);
        }
    }

    private void handleErrorCodes(LocationRequest.Valuation valuation, LocationRequest.DatasetState state,
                                  Response response, String body) {
        if (response.code() == HttpURLConnection.HTTP_UNAUTHORIZED || response.code() == HttpURLConnection.HTTP_FORBIDDEN) {
            throw new SparkServiceClient.SparkServiceException("Ingen tilgang til data access tjenesten", body);
        } else if (response.code() == HttpURLConnection.HTTP_NOT_FOUND) {
            throw new SparkServiceClient.SparkServiceException(String.format(
                    "Fant ingen location for valuation %s og state %s", valuation.name(), state.name()), body);
        } else if (response.code() < 200 || response.code() >= 400) {
            throw new SparkServiceClient.SparkServiceException("En feil har oppstått: " + response.toString(), body);
        }
    }

}
