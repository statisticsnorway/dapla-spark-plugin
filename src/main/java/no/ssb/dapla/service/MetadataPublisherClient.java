package no.ssb.dapla.service;

import io.opentracing.Span;
import io.opentracing.contrib.okhttp3.TracingInterceptor;
import io.opentracing.util.GlobalTracer;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.dapla.spark.plugin.token.SparkConfStore;
import no.ssb.dapla.spark.plugin.token.TokenRefresher;
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

public class MetadataPublisherClient {

    public static final String CONFIG_METADATA_PUBLISHER_URL = "spark.ssb.dapla.metadata.publisher.url";
    public static final String CONFIG_METADATA_PUBLISHER_PROJECT_ID = "spark.ssb.dapla.metadata.publisher.project.id";
    public static final String CONFIG_METADATA_PUBLISHER_TOPIC_NAME = "spark.ssb.dapla.metadata.publisher.topic.name";

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final OkHttpClient client;
    private final String baseURL;
    private final String projectId;
    private final String topicName;
    private final Span span;

    public MetadataPublisherClient(final SparkConf conf, Span span) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();

        SparkConfStore store;
        if (conf != null) {
            store = new SparkConfStore(conf);
        } else {
            store = SparkConfStore.get();
        }
        builder.addInterceptor(new OAuth2Interceptor(new TokenRefresher(store)));

        this.client = TracingInterceptor.addTracing(builder, GlobalTracer.get());
        String url = conf.get(CONFIG_METADATA_PUBLISHER_URL);
        if (!url.endsWith("/")) {
            this.baseURL = url + "/";
        } else {
            this.baseURL = url;
        }
        this.projectId = conf.get(CONFIG_METADATA_PUBLISHER_PROJECT_ID);
        this.topicName = conf.get(CONFIG_METADATA_PUBLISHER_TOPIC_NAME);
        this.span = span;
    }

    private String buildUrl(String format, Object... args) {
        return this.baseURL + String.format(format, args);
    }

    public void dataChanged(DatasetUri datasetUri, String filename) {
        DataChangedRequest dataChangedRequest = DataChangedRequest.newBuilder()
                .setProjectId(projectId)
                .setTopicName(topicName)
                .setUri(datasetUri.getParentUri() + datasetUri.getPath() + "/" + datasetUri.getVersion() + "/" + filename)
                .build();

        String body = ProtobufJsonUtils.toString(dataChangedRequest);
        span.log("DataChangedRequest" + body);

        Request request = new Request.Builder()
                .url(buildUrl("rpc/MetadataDistributorService/dataChanged"))
                .post(RequestBody.create(body, okhttp3.MediaType.get(MediaType.APPLICATION_JSON)))
                .build();
        try (Response response = client.newCall(request).execute()) {
            String json = getJson(response);
            handleErrorCodes(response, json);
        } catch (IOException e) {
            log.error("dataChanged failed", e);
            throw new MetadataPublisherException(e);
        } catch (Exception e) {
            log.error("dataChanged failed", e);
            throw e;
        }
    }

    private String getJson(Response response) throws IOException {
        ResponseBody body = response.body();
        if (body == null) return null;
        return body.string();
    }

    private void handleErrorCodes(Response response, String body) {
        if (response.code() < 200 || response.code() >= 400) {
            throw new MetadataPublisherException("En feil har oppstått: " + response.toString(), body);
        }
    }

    static class MetadataPublisherException extends RuntimeException {
        private final String body;

        public MetadataPublisherException(Throwable cause) {
            super(cause);
            this.body = null;
        }

        public MetadataPublisherException(String message, String body) {
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
