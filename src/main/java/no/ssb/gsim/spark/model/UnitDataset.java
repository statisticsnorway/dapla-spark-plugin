package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.gsim.spark.model.api.AbstractFetcher;
import okhttp3.HttpUrl;
import okhttp3.Request;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UnitDataset extends Dataset {

    public static final String UNIT_DATA_SET_NAME = "UnitDataSet";
    @JsonProperty
    private String unitDataStructure;

    public String getUnitDataStructure() {
        return unitDataStructure;
    }

    public void setUnitDataStructure(String unitDataStructure) {
        this.unitDataStructure = unitDataStructure;
    }

    public CompletableFuture<UnitDataStructure> fetchUnitDataStructure() {
        UnitDataStructure.Fetcher fetcher = new UnitDataStructure.Fetcher();
        fetcher.withParametersFrom(this);
        return fetcher.fetchAsync(getUnitDataStructure())
                .thenApply(result -> (UnitDataStructure) result.withParametersFrom(this));
    }

    public static class Fetcher extends AbstractFetcher<UnitDataset> {

        @Override
        public UnitDataset deserialize(ObjectMapper mapper, InputStream bytes) throws IOException {
            return mapper.readValue(bytes, UnitDataset.class);
        }

        @Override
        public Request.Builder getFetchRequest(HttpUrl prefix, String id, Long timestamp) {
            String normalizedId = id.replaceAll(UNIT_DATA_SET_NAME + "/", "");
            if (normalizedId.startsWith("/")) {
                normalizedId = normalizedId.substring(1);
            }
            HttpUrl url = prefix.resolve("./" + UNIT_DATA_SET_NAME + "/" + normalizedId);
            if (url == null) {
                throw new RuntimeException(new MalformedURLException());
            }
            Request.Builder builder = new Request.Builder();
            return builder.url(url);
        }

        @Override
        public Request.Builder getUpdateRequest(HttpUrl prefix, String id) {
            Request.Builder builder = new Request.Builder();
            HttpUrl url = prefix.resolve("./" + UNIT_DATA_SET_NAME + "/" + id);
            if (url == null) {
                throw new RuntimeException(new MalformedURLException());
            }
            return builder.url(url);
        }

        @Override
        public byte[] serialize(ObjectMapper mapper, UnitDataset object) throws IOException {
            return mapper.writeValueAsBytes(object);
        }
    }

}
