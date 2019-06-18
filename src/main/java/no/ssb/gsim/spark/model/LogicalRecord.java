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

    public List<String> getInstanceVariables() {
        return instanceVariables;
    }

    public void setInstanceVariables(List<String> instanceVariables) {
        this.instanceVariables = instanceVariables;
    }

    public CompletableFuture<List<InstanceVariable>> fetchInstanceVariables() {
        if (getInstanceVariables().isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        AbstractFetcher<InstanceVariable> fetcher = new InstanceVariable.Fetcher();
        fetcher.withParametersFrom(this);

        List<CompletableFuture<InstanceVariable>> fetches = getInstanceVariables().stream()
                .map(fetcher::fetchAsync).collect(Collectors.toList());

        return CompletableFuture.allOf(fetches.toArray(new CompletableFuture[0]))
                .thenApply(aVoid -> fetches.stream()
                        .map(CompletableFuture::join)
                        .map(result -> (InstanceVariable) result.withParametersFrom(this))
                        .collect(Collectors.toList())
                );
    }

    static class LogicalRecordFetcher extends AbstractFetcher<LogicalRecord> {

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
