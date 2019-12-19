package no.ssb.dapla.gcs.token.delegation;

import java.io.IOException;

import no.ssb.dapla.gcs.token.broker.BrokerAccessTokenProvider;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hadoop.fs.gcs.auth.AbstractDelegationTokenBinding;

// See https://github.com/GoogleCloudPlatform/gcp-token-broker/


public class BrokerDelegationTokenBinding extends AbstractDelegationTokenBinding {

    public BrokerDelegationTokenBinding() {
        super(BrokerTokenIdentifier.KIND);
    }

    @Override
    public AccessTokenProvider deployUnbonded() {
        return new BrokerAccessTokenProvider(getService());
    }

    @Override
    public AccessTokenProvider bindToTokenIdentifier(DelegationTokenIdentifier retrievedIdentifier) {
        return new BrokerAccessTokenProvider(
                getService(),
                (BrokerTokenIdentifier) retrievedIdentifier);
    }

    @Override
    public DelegationTokenIdentifier createTokenIdentifier() {
        return createEmptyIdentifier();
    }

    @Override
    public DelegationTokenIdentifier createTokenIdentifier(Text renewer) throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        String user = ugi.getUserName();
        Text owner = new Text(user);
        Text realUser = null;
        if (ugi.getRealUser() != null) {
            realUser = new Text(ugi.getRealUser().getUserName());
        }
        return new BrokerTokenIdentifier(
                getFileSystem().getConf(),
                owner,
                renewer,
                realUser,
                getService());
    }

    @Override
    public DelegationTokenIdentifier createEmptyIdentifier() {
        return new BrokerTokenIdentifier();
    }
}