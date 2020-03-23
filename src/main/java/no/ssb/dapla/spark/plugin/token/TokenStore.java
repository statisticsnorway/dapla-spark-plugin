package no.ssb.dapla.spark.plugin.token;

/**
 * Stores the tokens
 */
public interface TokenStore {

    String getAccessToken();

    void putAccessToken(String access);

    String getRefreshToken();

    void putRefreshToken(String refresh);
}
