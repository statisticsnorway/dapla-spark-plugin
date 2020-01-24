package no.ssb.dapla.spark.deploy.yarn.security;

import org.apache.hadoop.conf.Configuration;
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
        throw new RuntimeException("test obtainCredentials");
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
