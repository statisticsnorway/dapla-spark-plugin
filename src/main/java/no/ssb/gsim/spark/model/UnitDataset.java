package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.gsim.spark.model.api.AbstractFetcher;
import okhttp3.HttpUrl;
import okhttp3.Request;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UnitDataset extends Dataset {

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
        public Request getRequest(HttpUrl prefix, String id, Long timestamp) {
            Request.Builder builder = new Request.Builder();
            builder.header("Content-Type", "application/json");
            //HttpUrl base = HttpUrl.parse("https://lds.staging.ssbmod.net/ns/");
            HttpUrl url = prefix.resolve("./UnitDataSet/" + id);
            return builder.url(url).build();
        }

    }

}
