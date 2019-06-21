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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * https://lds-client.staging.ssbmod.net/be/lds-c/ns/UnitDataStructure/cd3ecd46-c090-4bc6-80fb-db758eeb10fd
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UnitDataStructure extends IdentifiableArtefact {

    public static final String UNIT_DATA_STRUCTURE_NAME = "UnitDataStructure";
    @JsonProperty
    private List<String> logicalRecords;

    public List<String> getLogicalRecords() {
        return logicalRecords;
    }

    public void setLogicalRecords(List<String> logicalRecords) {
        this.logicalRecords = logicalRecords;
    }

    public CompletableFuture<List<LogicalRecord>> fetchLogicalRecords() {
        if (getLogicalRecords().isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        LogicalRecord.Fetcher fetcher = new LogicalRecord.Fetcher();
        fetcher.withParametersFrom(this);

        List<CompletableFuture<LogicalRecord>> fetches = getLogicalRecords().stream()
                .map(fetcher::fetchAsync).collect(Collectors.toList());

        return CompletableFuture.allOf(fetches.toArray(new CompletableFuture[0]))
                .thenApply(aVoid -> fetches.stream()
                        .map(CompletableFuture::join)
                        .map(logicalRecord -> (LogicalRecord) logicalRecord.withParametersFrom(this))
                        .collect(Collectors.toList())
                );
    }

    public static class Fetcher extends AbstractFetcher<UnitDataStructure> {

        @Override
        public UnitDataStructure deserialize(ObjectMapper mapper, InputStream bytes) throws IOException {
            return mapper.readValue(bytes, UnitDataStructure.class);
        }

        @Override
        public Request.Builder getFetchRequest(HttpUrl prefix, String id, Long timestamp) {
            String normalizedId = id.replaceAll(UNIT_DATA_STRUCTURE_NAME + "/", "");
            if (normalizedId.startsWith("/")) {
                normalizedId = normalizedId.substring(1);
            }
            HttpUrl url = prefix.resolve("./" + UNIT_DATA_STRUCTURE_NAME + "/" + normalizedId);
            if (url == null) {
                throw new RuntimeException(new MalformedURLException());
            }
            Request.Builder builder = new Request.Builder();
            return builder.url(url);
        }

        @Override
        public Request.Builder getUpdateRequest(HttpUrl prefix, String id) {
            return null;
        }

        @Override
        public byte[] serialize(ObjectMapper mapper, UnitDataStructure object) throws IOException {
            return new byte[0];
        }
    }
}
