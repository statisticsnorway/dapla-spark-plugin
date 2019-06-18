package no.ssb.gsim.spark.model.api;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;

public interface Fetchable<T> {

    CompletableFuture<T> fetchAsync(String id);

    CompletableFuture<T> fetchAsync(String id, Long timestamp);

    default T fetch(String id) {
        return fetchAsync(id).join();
    }

    default T fetch(String id, Long timestamp) {
        return fetchAsync(id, timestamp).join();
    }

    default T fetch(String id, ZonedDateTime timestamp) {
        return fetch(id, timestamp.toInstant());
    }

    default T fetch(String id, Instant timestamp) {
        return fetch(id, timestamp.toEpochMilli());
    }

    default CompletableFuture<T> fetchAsync(String id, Instant timestamp) {
        return fetchAsync(id, timestamp.toEpochMilli());
    }

    default CompletableFuture<T> fetchAsync(String id, ZonedDateTime timestamp) {
        return fetchAsync(id, timestamp.toInstant());
    }
}
