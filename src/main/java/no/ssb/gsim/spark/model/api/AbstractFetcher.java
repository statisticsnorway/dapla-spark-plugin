package no.ssb.gsim.spark.model.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractFetcher<T> extends Configured implements Fetchable<T>, Deserializable<T>, Configurable {

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
        Request request = getRequest(getPrefix(), id, getTimestamp());
        Call call = getClient().newCall(request);
        FetcherCallback callback = new FetcherCallback();
        call.enqueue(callback);
        return callback;
    }

    public T deserialize(InputStream bytes) throws IOException {
        return deserialize(getMapper(), bytes);
    }

    @Override
    public abstract T deserialize(ObjectMapper mapper, InputStream bytes) throws IOException;

    public abstract Request getRequest(HttpUrl prefix, String id, Long timestamp);

    private final class FetcherCallback extends CompletableFuture<T> implements Callback {

        @Override
        public void onFailure(Call call, IOException e) {
            this.completeExceptionally(e);
        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
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
