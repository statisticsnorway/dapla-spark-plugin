package no.ssb.dapla.spark.deploy.yarn.security;

import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.spark.plugin.DaplaSparkConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.security.HadoopDelegationTokenProvider;
import org.apache.spark.deploy.yarn.security.ServiceCredentialProvider;
import scala.Option;

public class CredentialProvider implements ServiceCredentialProvider, HadoopDelegationTokenProvider {

    @Override
    public String serviceName() {
        System.out.println("serviceName is called");
        return "daplaCredentials";
    }

    @Override
    public boolean credentialsRequired(Configuration configuration) {
        return true;
    }

    @Override
    public Option<Object> obtainCredentials(Configuration configuration, SparkConf sparkConf, Credentials credentials) {
        Text service = new Text(DaplaSparkConfig.getHost(sparkConf));
        String namespace = sparkConf.get(BrokerTokenIdentifier.CURRENT_NAMESPACE);
        String operation = sparkConf.get(BrokerTokenIdentifier.CURRENT_OPERATION);
        credentials.addToken(service,
                BrokerDelegationTokenBinding.createUserToken(service, new Text(operation), new Text(namespace)));
        return Option.empty();
    }

    @Override
    public boolean delegationTokensRequired(SparkConf sparkConf, Configuration configuration) {
        return true;
    }

    @Override
    public Option<Object> obtainDelegationTokens(Configuration configuration, SparkConf sparkConf, Credentials credentials) {
        throw new RuntimeException("test obtainDelegationTokens");
    }
}
