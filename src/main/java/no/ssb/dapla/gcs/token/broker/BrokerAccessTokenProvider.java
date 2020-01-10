package no.ssb.dapla.gcs.token.broker;

import no.ssb.dapla.gcs.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AccessTokenProvider implementation that requires a "session token" represented by a BrokerTokenIdentifier.
 */
public final class BrokerAccessTokenProvider implements AccessTokenProvider {

    private Configuration config;
    private AccessToken accessToken;
    private BrokerTokenIdentifier tokenIdentifier;
    private Text service;
    private static final Logger LOG = LoggerFactory.getLogger(BrokerAccessTokenProvider.class);

    private final static AccessToken EXPIRED_TOKEN = new AccessToken("", -1L);

    public BrokerAccessTokenProvider(Text service, BrokerTokenIdentifier bti) {
        this.service = service;
        this.tokenIdentifier = bti;
        this.accessToken = EXPIRED_TOKEN;
    }

    @Override
    public AccessToken getAccessToken() {
        return this.accessToken;
    }

    @Override
    public void refresh() {

        validateTokenIdentifier();
        /*
        GetAccessTokenResponse response = loginUser.doAs((PrivilegedAction<GetAccessTokenResponse>) () -> {
            BrokerGateway gateway = new BrokerGateway(config, sessionToken);
            GetAccessTokenRequest request = GetAccessTokenRequest.newBuilder()
                    .setScope(BrokerTokenIdentifier.BROKER_SCOPE)
                    .setOwner(currentUser.getUserName())
                    .setTarget(service.toString())
                    .build();
            GetAccessTokenResponse r = gateway.getStub().getAccessToken(request);
            gateway.getManagedChannel().shutdown();
            return r;'
        });

        String tokenString = response.getAccessToken();
        long expiresAt = response.getExpiresAt();
        accessToken = new AccessToken(tokenString, expiresAt);
         */
        LOG.debug("Issuing access token for service: " + this.service);
        try {
            // TODO: Turn useComputeEngineFallback OFF when the following has been resolved: https://statistics-norway.atlassian.net/browse/BIP-379
            GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true, BrokerTokenIdentifier.BROKER_SCOPE);
            accessToken =  new AccessToken(credential.getAccessToken(), credential.getExpirationTime());
        } catch (Exception e) {
            throw new RuntimeException("GoogleCredentialsFactory failed", e);
        }
    }

    private void validateTokenIdentifier() {
        if (tokenIdentifier == null) {
            throw new IllegalStateException("Invalid session. Cannot find required token identifier.");
        }

        if (config == null || config.get(BrokerTokenIdentifier.CURRENT_NAMESPACE) == null ||
                config.get(BrokerTokenIdentifier.CURRENT_OPERATION) == null) {
            throw new IllegalStateException("Invalid session. Cannot get current namespace or operation.");
        }

        if (tokenIdentifier.getOperation().equals(config.get(BrokerTokenIdentifier.CURRENT_OPERATION))) {
            throw new IllegalStateException(String.format(
                    "Invalid session. Current operation %s does not match the token identifier operation %s",
                    config.get(BrokerTokenIdentifier.CURRENT_OPERATION), tokenIdentifier.getOperation()));
        }

        if (tokenIdentifier.getNamespace().equals(config.get(BrokerTokenIdentifier.CURRENT_NAMESPACE))) {
            throw new IllegalStateException(String.format(
                    "Invalid session. Current namespace %s does not match the token identifier namespace %s",
                    config.get(BrokerTokenIdentifier.CURRENT_NAMESPACE), tokenIdentifier.getNamespace()));
        }
    }

    @Override
    public void setConf(Configuration config) {
        this.config = config;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }

}