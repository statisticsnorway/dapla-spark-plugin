package no.ssb.dapla.gcs.token.delegation;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase;
import com.google.cloud.hadoop.fs.gcs.auth.AbstractDelegationTokenBinding;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import no.ssb.dapla.gcs.token.broker.BrokerAccessTokenProvider;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * A DelegationTokenBinding implementation that binds a file system to a BrokerAccessTokenProvider.
 * Note that the BrokerAccessTokenProvider requires a "session token" (which is issued to the logged in user), so any
 * attempt to obtain an access token without a DelegationTokenIdentifier ("session token") will fail.
 */
public class BrokerDelegationTokenBinding extends AbstractDelegationTokenBinding {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerDelegationTokenBinding.class);

    public BrokerDelegationTokenBinding() {
        super(BrokerTokenIdentifier.KIND);
    }

    /**
     * Creates a security token ("session token") that can be added to the logged in user.
     *
     * @param service name of the service (i.e. bucket name) for the FS.
     * @param operation the operation (read or write)
     * @param namespace the namespace
     * @param realUser the real username of the token owner
     * @return the token
     */
    public static Token<DelegationTokenIdentifier> createHadoopToken(Text service, Text operation, Text namespace,
                                                                     Text realUser) {
        BrokerDelegationTokenBinding binding = new BrokerDelegationTokenBinding();
        DelegationTokenIdentifier tokenIdentifier = new BrokerTokenIdentifier.Builder()
                .withService(service)
                .withOperation(operation)
                .withNamespace(namespace)
                //.withRealUser(realUser)
                .build();

        Token<DelegationTokenIdentifier> token = new Token<>(tokenIdentifier, binding.secretManager);
        token.setKind(binding.getKind());
        token.setService(service);
        LOG.debug("Created user token: " + token);
        System.out.println("Created user token: " + token);
        return token;
    }

    @Override
    public AccessTokenProvider deployUnbonded() {
        // This DelegationTokenBinding implementation requires a DelegationTokenIdentifier.
        // When this method is called, it means that the file system cannot find a delegation token, and instead
        // tries to use direct authentication.
        String trace = " trace failed ";
        try {
            //LOG.debug("Real user: " + UserGroupInformation.getCurrentUser().getRealUser().getUserName());
            trace = "Token for " + UserGroupInformation.getCurrentUser().getUserName()
                    + ": ";
            Iterator iter = UserGroupInformation.getCurrentUser().getTokens().iterator();
            while (iter.hasNext()) {
                trace += iter.next() + " ";
            }
            System.out.println(trace);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        throw new IllegalStateException("This operation is not allowed. Trace: " + trace);
    }

    @Override
    public AccessTokenProvider bindToTokenIdentifier(DelegationTokenIdentifier retrievedIdentifier) {
        LOG.debug("bindToTokenIdentifier");
        try {
            String trace = "bindToTokenIdentifier tokens " + UserGroupInformation.getCurrentUser().getUserName()
                    + ": ";
            Iterator iter = UserGroupInformation.getCurrentUser().getTokens().iterator();
            while (iter.hasNext()) {
                trace += iter.next() + " ";
            }
            System.out.println(trace);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new BrokerAccessTokenProvider(getService(), (BrokerTokenIdentifier) retrievedIdentifier);
    }

    @Override
    public void bindToFileSystem(GoogleHadoopFileSystemBase fs, Text service) {
        super.bindToFileSystem(fs, service);
    }

    @Override
    public DelegationTokenIdentifier createTokenIdentifier() {
        return createEmptyIdentifier();
    }

    @Override
    public DelegationTokenIdentifier createTokenIdentifier(Text renewer) {
        // Should not be used
        return new BrokerTokenIdentifier();
    }

    @Override
    public DelegationTokenIdentifier createEmptyIdentifier() {
        // Should not be used
        return new BrokerTokenIdentifier();
    }
}