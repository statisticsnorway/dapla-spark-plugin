package no.ssb.gsim.spark.model.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractFetcher<T> extends Configured implements Fetchable<T>, Updatable<T>, Deserializable<T>,
        Serializable<T>, Configurable {

    public static final String APPLICATION_JSON_STRING = "application/json; charset=utf-8";
    public static final MediaType APPLICATION_JSON = MediaType.parse(APPLICATION_JSON_STRING);
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    public T fetch(String id) {
        return fetch(id, getTimestamp());
    }

    @Override
    public CompletableFuture<T> fetchAsync(String id) {
        return fetchAsync(id, getTimestamp());
    }

    @Override
    public CompletableFuture<T> fetchAsync(String id, Long timestamp) {
        Request.Builder request = getFetchRequest(getPrefix(), id, getTimestamp());
        request.header("Accept", APPLICATION_JSON_STRING);
        Call call = getClient().newCall(request.build());
        FetcherCallback callback = new FetcherCallback();
        call.enqueue(callback);
        return callback;
    }

    @Override
    public CompletableFuture<Void> updateAsync(String id, T object) throws IOException {
        byte[] bytes = serialize(getMapper(), object);
        Request.Builder updateRequest = getUpdateRequest(getPrefix(), id);
        updateRequest.put(RequestBody.create(APPLICATION_JSON, bytes));
        Call call = getClient().newCall(updateRequest.build());
        CompletableFuture<Void> future = new CompletableFuture<>();
        call.enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                future.completeExceptionally(e);
            }

            @Override
            public void onResponse(Call call, Response response) {
                if (response.isSuccessful()) {
                    future.complete(null);
                }
                future.completeExceptionally(new IOException("http error: " + response.message()));
            }
        });
        return future;
    }

    private T deserialize(InputStream bytes) throws IOException {
        return deserialize(getMapper(), bytes);
    }

    @Override
    public abstract T deserialize(ObjectMapper mapper, InputStream bytes) throws IOException;

    public abstract Request.Builder getFetchRequest(HttpUrl prefix, String id, Long timestamp);

    public abstract Request.Builder getUpdateRequest(HttpUrl prefix, String id);


    private final class FetcherCallback extends CompletableFuture<T> implements Callback {

        @Override
        public void onFailure(Call call, IOException e) {
            this.completeExceptionally(e);
        }

        @Override
        public void onResponse(Call call, Response response) {
            try {
                if (!response.isSuccessful()) {
                    throw new IOException("http error: " + response.message());
                }
                ResponseBody body = response.body();
                if (body == null) {
                    throw new IOException("empty body");
                }
                this.complete(deserialize(response.body().byteStream()));
            } catch (Throwable e) {
                LOG.warn("failed to deserialize body of {}", call.request());
                this.completeExceptionally(e);
            }
        }
    }
}
