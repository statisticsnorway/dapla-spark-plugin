package no.ssb.dapla.service;

import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.dapla.spark.plugin.metadata.FilesystemMetaDataWriter;
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

    public MetadataPublisherClient(final SparkConf conf) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        OAuth2Interceptor.createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        this.client = builder.build();
        String url = conf.get(CONFIG_METADATA_PUBLISHER_URL);
        if (!url.endsWith("/")) {
            this.baseURL = url + "/";
        } else {
            this.baseURL = url;
        }
        this.projectId = conf.get(CONFIG_METADATA_PUBLISHER_PROJECT_ID);
        this.topicName = conf.get(CONFIG_METADATA_PUBLISHER_TOPIC_NAME);
    }

    private String buildUrl(String format, Object... args) {
        return this.baseURL + String.format(format, args);
    }

    public void dataChanged(DatasetUri datasetUri) {
        DataChangedRequest dataChangedRequest = DataChangedRequest.newBuilder()
                .setParentUri(datasetUri.getParentUri())
                .setPath(datasetUri.getPath())
                .setVersion(Long.parseLong(datasetUri.getVersion()))
                .setProjectId(projectId)
                .setTopicName(topicName)
                .setFilename(FilesystemMetaDataWriter.DATASET_META_FILE_NAME)
                .build();

        String body = ProtobufJsonUtils.toString(dataChangedRequest);

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
