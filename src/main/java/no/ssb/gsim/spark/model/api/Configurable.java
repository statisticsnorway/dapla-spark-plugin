package no.ssb.gsim.spark.model.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.time.Instant;

public interface Configurable {

    @JsonIgnore
    Configurable withMapper(ObjectMapper mapper);

    @JsonIgnore
    Configurable withClient(OkHttpClient client);

    @JsonIgnore
    Configurable withPrefix(HttpUrl prefix);

    @JsonIgnore
    Configurable withTimestamp(Long timestamp);

    @JsonIgnore
    default Configurable withTimestamp(Instant timestamp) {
        return withTimestamp(timestamp.toEpochMilli());
    }

    @JsonIgnore
    default Configurable withParametersFrom(Configured configured) {
        return this.withClient(configured.getClient())
                .withMapper(configured.getMapper())
                .withPrefix(configured.getPrefix())
                .withTimestamp(configured.getTimestamp());
    }
}
