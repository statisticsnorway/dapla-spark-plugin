package no.ssb.dapla.spark.plugin;

import org.apache.spark.SparkConf;

public class DaplaSparkConfig {

    private static final String CONFIG = "spark.ssb.dapla.";
    public static final String SPARK_SSB_DAPLA_GCS_STORAGE = "spark.ssb.dapla.gcs.storage";
    public static final String SPARK_SSB_DAPLA_OUTPUT_PREFIX = "spark.ssb.dapla.output.prefix";
    public static final String SPARK_SSB_USERNAME = "spark.ssb.username";
    public static final String SPARK_SSB_ACCESS_TOKEN = "spark.ssb.access";
    public static final String SPARK_SSB_RENEW_TOKEN = "spark.ssb.renew";
    public static final String CONFIG_ROUTER_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    public static final String CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE = CONFIG + "oauth.credentials.file";
    public static final String CONFIG_ROUTER_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    public static final String CONFIG_ROUTER_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";

    static final String FS_GS_IMPL_DISABLE_CACHE = "fs.gs.impl.disable.cache";

    SparkConf conf;

    public DaplaSparkConfig(SparkConf sparkConf) {
        this.conf = sparkConf;
    }

    public String getStoragePath() {
        throwExceptionIfNotExist(SPARK_SSB_DAPLA_GCS_STORAGE);
        return conf.get(SPARK_SSB_DAPLA_GCS_STORAGE);
    }

    public static String getStoragePath(SparkConf conf) {
        throwExceptionIfNotExist(conf, SPARK_SSB_DAPLA_GCS_STORAGE);
        return conf.get(SPARK_SSB_DAPLA_GCS_STORAGE);
    }

    String getOutputOathPrefix() {
        return conf.get(SPARK_SSB_DAPLA_OUTPUT_PREFIX, "datastore/output");
    }

    private void throwExceptionIfNotExist(String key) {
        throwExceptionIfNotExist(conf, key);
    }

    static void throwExceptionIfNotExist(SparkConf conf, String key) {
        if (!conf.contains(key)) {
            throw new IllegalStateException(key + " not found in spark config\n" + conf.toDebugString());
        }
    }

}
