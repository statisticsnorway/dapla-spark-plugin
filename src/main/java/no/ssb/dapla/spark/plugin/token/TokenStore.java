package no.ssb.dapla.spark.plugin.token;

import java.util.Optional;

/**
 * Stores the tokens
 */
public interface TokenStore {

    Optional<String> getClientId();

    Optional<String> getClientSecret();

    String getTokenUrl();

    String getAccessToken();

    void putAccessToken(String access);

    String getRefreshToken();

    void putRefreshToken(String refresh);
}
