package no.ssb.dapla.spark.plugin;

import org.apache.spark.SparkConf;

public class DaplaSparkConfig {
    static final String SPARK_SSB_DAPLA_GCS_STORAGE = "spark.ssb.dapla.gcs.storage";
    static final String SPARK_SSB_DAPLA_OUTPUT_PREFIX = "spark.ssb.dapla.output.prefix";
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
