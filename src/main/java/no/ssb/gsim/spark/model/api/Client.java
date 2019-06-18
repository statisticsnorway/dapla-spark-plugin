package no.ssb.gsim.spark.model.api;

import no.ssb.gsim.spark.model.UnitDataset;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class Client extends Configured {

    public CompletableFuture<UnitDataset> fetchUnitDataset(String id, Instant timestamp) {
        UnitDataset.Fetcher fetcher = new UnitDataset.Fetcher();
        fetcher.withParametersFrom(this).withTimestamp(timestamp);
        return fetcher.fetchAsync(id).thenApply(result -> (UnitDataset) result.withParametersFrom(this));
    }
}
