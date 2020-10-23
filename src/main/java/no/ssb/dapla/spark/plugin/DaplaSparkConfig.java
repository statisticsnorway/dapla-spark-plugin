package no.ssb.dapla.spark.plugin;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class DaplaSparkConfig {

    private static final String CONFIG = "spark.ssb.dapla.";
    public static final String SPARK_SSB_DAPLA_GCS_STORAGE = "spark.ssb.dapla.gcs.storage";
    public static final String SPARK_SSB_DAPLA_DEFAULT_PARTITION_SIZE = "spark.ssb.dapla.default.partition.size";
    public static final String SPARK_SSB_ACCESS_TOKEN = "spark.ssb.access";
    public static final String SPARK_SSB_REFRESH_TOKEN = "spark.ssb.refresh";
    public static final String CONFIG_ROUTER_OAUTH_TOKEN_IGNORE_EXPIRY = CONFIG + "oauth.ignoreExpiry";
    public static final String CONFIG_ROUTER_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    public static final String CONFIG_ROUTER_OAUTH_TRACING_URL = CONFIG + "oauth.tracingUrl";
    public static final String CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE = CONFIG + "oauth.credentials.file";
    public static final String CONFIG_ROUTER_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    public static final String CONFIG_ROUTER_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";

    static final String FS_GS_IMPL_DISABLE_CACHE = "fs.gs.impl.disable.cache";

    SparkConf conf;

    public DaplaSparkConfig(SparkConf sparkConf) {
        this.conf = sparkConf;
    }

}
