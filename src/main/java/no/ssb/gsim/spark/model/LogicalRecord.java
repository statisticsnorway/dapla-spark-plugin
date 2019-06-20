package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.gsim.spark.model.api.AbstractFetcher;
import okhttp3.HttpUrl;
import okhttp3.Request;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LogicalRecord extends IdentifiableArtefact {

    @JsonProperty
    private List<String> instanceVariables;

    @JsonProperty
    private String shortName;

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public List<String> getInstanceVariables() {
        return instanceVariables;
    }

    public void setInstanceVariables(List<String> instanceVariables) {
        this.instanceVariables = instanceVariables;
    }

    static class Fetcher extends AbstractFetcher<LogicalRecord> {

        @Override
        public LogicalRecord deserialize(ObjectMapper mapper, InputStream bytes) throws IOException {
            return mapper.readValue(bytes, LogicalRecord.class);
        }

        @Override
        public Request getRequest(HttpUrl prefix, String id, Long timestamp) {

            String normalizedId = id.replaceAll("LogicalRecord/", "");
            if (normalizedId.startsWith("/")) {
                normalizedId = normalizedId.substring(1);
            }

            Request.Builder builder = new Request.Builder();
            builder.header("Content-Type", "application/json");
            //HttpUrl base = HttpUrl.parse("https://lds.staging.ssbmod.net/ns/");
            HttpUrl url = prefix.resolve("./LogicalRecord/" + normalizedId);
            return builder.url(url).build();
        }
    }
}
