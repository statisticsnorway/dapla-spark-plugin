package no.ssb.dapla.gcs.token;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AccessTokenProvider implementation that retrieves an access token from spark.
 */
public final class SparkAccessTokenProvider implements AccessTokenProvider {

    private Configuration config;
    private AccessToken accessToken;
    private static final Logger LOG = LoggerFactory.getLogger(SparkAccessTokenProvider.class);

    private final static AccessToken EXPIRED_TOKEN = new AccessToken("", -1L);

    public SparkAccessTokenProvider() {
        this.accessToken = EXPIRED_TOKEN;
    }

    @Override
    public AccessToken getAccessToken() {
        return this.accessToken;
    }

    @Override
    public void refresh() {
        accessToken = new AccessToken(config.get(SparkOptions.ACCESS_TOKEN), Long.valueOf(config.get(SparkOptions.ACCESS_TOKEN_EXP)));
    }

    @Override
    public void setConf(Configuration config) {
        this.config = config;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }

}