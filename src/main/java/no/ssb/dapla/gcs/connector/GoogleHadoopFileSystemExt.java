package no.ssb.dapla.gcs.connector;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;

public class GoogleHadoopFileSystemExt extends GoogleHadoopFileSystem {

    @Override
    public void initialize(URI path, Configuration config) throws IOException {
        if (!hasSufficientSparkConfig(config)) {
            throw new IllegalStateException("Invalid session. Cannot get current namespace or operation.");
        }
        super.initialize(path, config);
    }

    private boolean hasSufficientSparkConfig(Configuration config) {
        return config.get(SparkOptions.ACCESS_TOKEN, null) != null &&
                config.get(SparkOptions.ACCESS_TOKEN_EXP, null) != null;
    }
}
