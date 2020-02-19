package no.ssb.dapla.gcs.connector;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

public class GoogleHadoopFileSystemExt extends GoogleHadoopFileSystem {

    @Override
    public void initialize(URI path, Configuration config) throws IOException {
        Text service = new Text(this.getScheme() + "://" + path.getAuthority());
        Token<?> token = UserGroupInformation.getCurrentUser().getCredentials().getToken(service);
        if (token == null) {
            // Initialise delegation token
            String operation = config.get(SparkOptions.CURRENT_OPERATION);
            String namespace = config.get(SparkOptions.CURRENT_NAMESPACE);
            String userId = config.get(SparkOptions.CURRENT_USER);
            UserGroupInformation.getCurrentUser().addToken(service,
                    BrokerDelegationTokenBinding.createHadoopToken(service, new Text(operation),
                            new Text(namespace), new Text(userId)));
        }
        super.initialize(path, config);
    }

    /*
    @Override
    public Text getDelegationTokenLookupService(URI path) {
        String operation = getConf().get("spark.ssb.session.operation");
        String namespace = getConf().get("spark.ssb.session.namespace");
        return new Text(operation + ":" + namespace);
    }
     */

    @Override
    public FileStatus getFileStatus(Path hadoopPath) throws IOException {
        return super.getFileStatus(hadoopPath);
    }

    @Override
    public FSDataInputStream open(Path hadoopPath, int bufferSize) throws IOException {
        String operation = getConf().get(SparkOptions.CURRENT_OPERATION);
        String namespace = getConf().get("spark.ssb.session.namespace");
        Token<DelegationTokenIdentifier> boundToken = delegationTokens.getBoundDT();
        if (boundToken != null) {
            BrokerTokenIdentifier brokerTokenIdentifier = (BrokerTokenIdentifier) boundToken.decodeIdentifier();
            if (!brokerTokenIdentifier.getOperation().toString().equals(operation)) {
                System.out.println("Current token has different operation: " + brokerTokenIdentifier.getOperation().toString());
                this.setGcsFs(this.createGcsFs(getConf()));
            }
            if (!brokerTokenIdentifier.getNamespace().toString().equals(namespace)) {
                System.out.println("Current token has different namespace: " + brokerTokenIdentifier.getNamespace().toString());
                this.setGcsFs(this.createGcsFs(getConf()));
            }
        }
        return super.open(hadoopPath, bufferSize);
    }

    @Override
    public FSDataOutputStream create(Path hadoopPath, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        String operation = getConf().get(SparkOptions.CURRENT_OPERATION);
        if (!AccessTokenRequest.Privilege.WRITE.name().equals(operation)) {
            this.setGcsFs(this.createGcsFs(getConf()));
        }
        return super.create(hadoopPath, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

}
