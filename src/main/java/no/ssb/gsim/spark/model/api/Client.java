package no.ssb.gsim.spark.model.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.gsim.spark.model.UnitDataset;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class Client extends Configured {

    public CompletableFuture<UnitDataset> fetchUnitDataset(String id, Instant timestamp) {
        UnitDataset.Fetcher fetcher = new UnitDataset.Fetcher();
        fetcher.withParametersFrom(this).withTimestamp(timestamp);
        return fetcher.fetchAsync(id).thenApply(result -> (UnitDataset) result.withParametersFrom(this));
    }

    @Override
    public Client withMapper(ObjectMapper mapper) {
        return (Client) super.withMapper(mapper);
    }

    @Override
    public Client withClient(OkHttpClient client) {
        return (Client) super.withClient(client);
    }

    @Override
    public Client withPrefix(HttpUrl prefix) {
        return (Client) super.withPrefix(prefix);
    }

    @Override
    public Client withTimestamp(Long timestamp) {
        return (Client) super.withTimestamp(timestamp);
    }

    @Override
    public Client withTimestamp(Instant timestamp) {
        return (Client) super.withTimestamp(timestamp);
    }

    @Override
    public Client withParametersFrom(Configured configured) {
        return (Client) super.withParametersFrom(configured);
    }
}
