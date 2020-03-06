package no.ssb.dapla.gcs.token.broker;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import no.ssb.dapla.data.access.protobuf.DatasetState;
import no.ssb.dapla.data.access.protobuf.Privilege;
import no.ssb.dapla.data.access.protobuf.Valuation;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsDetails;
import no.ssb.dapla.gcs.oauth.GoogleCredentialsFactory;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;

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
        LOG.debug("Issuing access token for service: " + this.service);
        try {
            if (useLocalCredentials()) {
                System.out.println("Using local credentials file");
                final String scope = "https://www.googleapis.com/auth/devstorage.read_write";
                GoogleCredentialsDetails credential = GoogleCredentialsFactory.createCredentialsDetails(true, scope);
                accessToken = new AccessToken(credential.getAccessToken(), credential.getExpirationTime());
            } else {
                DataAccessClient dataAccessClient = new DataAccessClient(this.config);
                Privilege privilege = Privilege.valueOf(config.get(SparkOptions.CURRENT_OPERATION));
                String path = config.get(SparkOptions.CURRENT_NAMESPACE);
                Valuation valuation = ofNullable(config.get(SparkOptions.CURRENT_DATASET_VALUATION))
                        .filter(v -> !v.trim().isEmpty())
                        .map(Valuation::valueOf)
                        .orElse(null);
                DatasetState state = ofNullable(config.get(SparkOptions.CURRENT_DATASET_STATE))
                        .filter(v -> !v.trim().isEmpty())
                        .map(DatasetState::valueOf)
                        .orElse(null);
                accessToken = dataAccessClient.getAccessToken(path, 0, privilege, valuation, state);
            }
        } catch (Exception e) {
            throw new RuntimeException("Issuing access token failed for service: " + this.service, e);
        }
    }

    private boolean useLocalCredentials() {
        return config.getBoolean("spark.ssb.use.local.credentials", false);
    }

    private void validateTokenIdentifier() {
        if (tokenIdentifier == null) {
            throw new IllegalStateException("Invalid session. Cannot find required token identifier.");
        }

        if (config == null || config.get(SparkOptions.CURRENT_NAMESPACE) == null ||
                config.get(SparkOptions.CURRENT_OPERATION) == null) {
            throw new IllegalStateException("Invalid session. Cannot get current namespace or operation.");
        }
        System.out.println("tokenIdentifier " + tokenIdentifier);
        if (!tokenIdentifier.getOperation().toString().equals(config.get(SparkOptions.CURRENT_OPERATION))) {
            throw new IllegalStateException(String.format(
                    "Invalid session. Current operation %s does not match the token identifier operation %s",
                    config.get(SparkOptions.CURRENT_OPERATION), tokenIdentifier.getOperation()));
        }

        if (!tokenIdentifier.getNamespace().toString().equals(config.get(SparkOptions.CURRENT_NAMESPACE))) {
            throw new IllegalStateException(String.format(
                    "Invalid session. Current namespace %s does not match the token identifier namespace %s",
                    config.get(SparkOptions.CURRENT_NAMESPACE), tokenIdentifier.getNamespace()));
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