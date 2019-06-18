package no.ssb.gsim.spark.model.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;


public class Configured implements Configurable {

    @JsonIgnore
    private ObjectMapper mapper;

    @JsonIgnore
    private OkHttpClient client;

    @JsonIgnore
    private HttpUrl prefix;

    @JsonIgnore
    private Long timestamp;

    @Override
    public Configurable withMapper(ObjectMapper mapper) {
        this.mapper = mapper;
        return this;
    }

    @Override
    public Configurable withClient(OkHttpClient client) {
        this.client = client;
        return this;
    }

    @Override
    public Configurable withPrefix(HttpUrl prefix) {
        this.prefix = prefix;
        return this;
    }

    @Override
    public Configurable withTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public OkHttpClient getClient() {
        return this.client;
    }

    public ObjectMapper getMapper() {
        return this.mapper;
    }

    public HttpUrl getPrefix() {
        return this.prefix;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }
}
