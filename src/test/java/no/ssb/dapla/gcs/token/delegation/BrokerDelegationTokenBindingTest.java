package no.ssb.dapla.gcs.token.delegation;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.DELEGATION_TOKEN_BINDING_CLASS;
import static org.assertj.core.api.Assertions.*;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class BrokerDelegationTokenBindingTest {

    @After
    public void tearDown() {
        UserGroupInformation.setLoginUser(null);
    }

    /** Verifies that a configured delegation token binding is correctly loaded and employed */
    @Test
    public void testDelegationTokenBinding() throws IOException {
        URI initUri = new Path("gs://test").toUri();
        Text expectedKind = BrokerTokenIdentifier.KIND;

        GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
        loadUserConfig(new Text(initUri.toString()));
        fs.initialize(initUri, loadConfig());

        // Request a delegation token
        Token<?> dt = fs.getDelegationToken(null);
        assertThat(dt.getService().toString()).isEqualTo(initUri.toString());
        assertThat(dt.getService().toString()).isEqualTo(initUri.toString());
        assertThat(dt.getKind()).isEqualTo(expectedKind);
        assertThat(dt.decodeIdentifier()).isExactlyInstanceOf(BrokerTokenIdentifier.class);

        BrokerTokenIdentifier token = (BrokerTokenIdentifier) dt.decodeIdentifier();
        assertThat(token.getOperation().toString()).isEqualTo("operation");
        assertThat(token.getNamespace().toString()).isEqualTo("ssb.test.namespace");
    }

    @Test(expected = RuntimeException.class)
    public void unboundedOperationShouldFail() throws IOException {
        URI initUri = new Path("gs://test").toUri();
        GoogleHadoopFileSystem fs = new GoogleHadoopFileSystem();
        // Without a user and a session token this should fail
        fs.initialize(initUri, loadConfig());
    }

    private Configuration loadConfig() {
        Configuration config = new Configuration();

        config.set(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), "test_project");
        config.setInt(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_BUFFER_SIZE.getKey(), 512);
        config.setLong(GoogleHadoopFileSystemConfiguration.BLOCK_SIZE.getKey(), 1024);

        // Token binding config
        config.set(DELEGATION_TOKEN_BINDING_CLASS.getKey(), BrokerDelegationTokenBinding.class.getName());
        return config;
    }

    private void loadUserConfig(Text service) throws IOException {
        UserGroupInformation.getCurrentUser().addToken(service, BrokerDelegationTokenBinding.createHadoopToken(service,
                new Text("operation"), new Text("ssb.test.namespace"), new Text("realUserName")));
    }
}
