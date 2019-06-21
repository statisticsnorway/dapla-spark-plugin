package no.ssb.gsim.spark.model.api;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface Updatable<T> {

    CompletableFuture<Void> updateAsync(String id, T object) throws IOException;

    default void update(String id, T object) throws IOException {
        updateAsync(id, object).join();
    }
}
