package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.spark.plugin.token.TokenSupplier;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Objects;

/**
 * Interceptor that adds a <p>Bearer</p> token.
 */
public class OAuth2Interceptor implements Interceptor {

    private static final String AUTHORIZATION_HEADER_NAME = "Authorization";
    private final TokenSupplier supplier;

    /**
     * Create new instance
     *
     * @param supplier the token supplier
     */
    public OAuth2Interceptor(TokenSupplier supplier) {
        this.supplier = Objects.requireNonNull(supplier);
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request.Builder newRequest = chain.request().newBuilder();
        newRequest.header(AUTHORIZATION_HEADER_NAME, String.format("Bearer %s", supplier.get()));
        return chain.proceed(newRequest.build());
    }
}