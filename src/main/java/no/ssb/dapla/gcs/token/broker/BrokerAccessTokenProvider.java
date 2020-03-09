package no.ssb.dapla.gcs.token.broker;

import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.protobuf.ByteString;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

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
            DataAccessClient dataAccessClient = new DataAccessClient(this.config);

            String operation = config.get(SparkOptions.CURRENT_OPERATION);
            String path = config.get(SparkOptions.CURRENT_NAMESPACE);

            if ("READ".equals(operation)) {

                String version = config.get(SparkOptions.CURRENT_DATASET_VERSION);
                ReadAccessTokenResponse readAccessTokenResponse = dataAccessClient.readAccessToken(ReadAccessTokenRequest.newBuilder()
                        .setPath(path)
                        .setVersion(version)
                        .build());
                accessToken = new AccessToken(readAccessTokenResponse.getAccessToken(), readAccessTokenResponse.getExpirationTime());

            } else if ("WRITE".equals(operation)) {

                String datasetMetaJson = config.get(SparkOptions.CURRENT_DATASET_META_JSON);
                String datasetMetaSignatureBase64 = config.get(SparkOptions.CURRENT_DATASET_META_JSON_SIGNATURE);
                byte[] datasetMetaSignatureBytes = Base64.getDecoder().decode(datasetMetaSignatureBase64);
                WriteAccessTokenResponse writeAccessTokenResponse = dataAccessClient.writeAccessToken(WriteAccessTokenRequest.newBuilder()
                        .setMetadataJson(ByteString.copyFromUtf8(datasetMetaJson))
                        .setMetadataSignature(ByteString.copyFrom(datasetMetaSignatureBytes))
                        .build());
                accessToken = new AccessToken(writeAccessTokenResponse.getAccessToken(), writeAccessTokenResponse.getExpirationTime());
            }
        } catch (Exception e) {
            throw new RuntimeException("Issuing access token failed for service: " + this.service, e);
        }
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