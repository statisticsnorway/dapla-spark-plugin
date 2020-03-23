package no.ssb.dapla.spark.plugin.token;

import org.apache.spark.SparkConf;

import java.util.Objects;

import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.SPARK_SSB_ACCESS_TOKEN;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.SPARK_SSB_REFRESH_TOKEN;

public class SparkConfStore implements TokenStore {

    private final SparkConf conf;

    public SparkConfStore(SparkConf conf) {
        this.conf = Objects.requireNonNull(conf);
    }

    @Override
    public String getAccessToken() {
        return Objects.requireNonNull(conf.get(SPARK_SSB_ACCESS_TOKEN, null), "missing access token");
    }

    @Override
    public String getRefreshToken() {
        return Objects.requireNonNull(conf.get(SPARK_SSB_REFRESH_TOKEN, null), "missing refresh token");
    }

    @Override
    public void putAccessToken(String access) {
        conf.set(SPARK_SSB_ACCESS_TOKEN, Objects.requireNonNull(access, "access token cannot be null"));
    }

    @Override
    public void putRefreshToken(String refresh) {
        conf.set(SPARK_SSB_REFRESH_TOKEN, Objects.requireNonNull(refresh, "refresh token cannot be null"));
    }
}
