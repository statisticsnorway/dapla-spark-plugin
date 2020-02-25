package no.ssb.dapla.gcs.connector;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;

public class GoogleHadoopFileSystemExt extends GoogleHadoopFileSystem {

    @Override
    public void initialize(URI path, Configuration config) throws IOException {
        Text service = new Text(this.getScheme() + "://" + path.getAuthority());
        if (hasSufficientSparkConfig(config)) {
            // Initialise delegation token
            System.out.println("Initialise delegation token");
            String operation = config.get(SparkOptions.CURRENT_OPERATION);
            String namespace = config.get(SparkOptions.CURRENT_NAMESPACE);
            String userId = config.get(SparkOptions.CURRENT_USER);
            UserGroupInformation.getCurrentUser().addToken(service,
                    BrokerDelegationTokenBinding.createHadoopToken(service, new Text(operation),
                            new Text(namespace), new Text(userId)));
        } else {
            throw new IllegalStateException("Invalid session. Cannot get current namespace or operation.");
        }
        super.initialize(path, config);
    }

    private boolean hasSufficientSparkConfig(Configuration config) {
        return config.get(SparkOptions.CURRENT_OPERATION, null) != null &&
                config.get(SparkOptions.CURRENT_NAMESPACE, null) != null &&
                config.get(SparkOptions.CURRENT_USER, null) != null;
    }
}
