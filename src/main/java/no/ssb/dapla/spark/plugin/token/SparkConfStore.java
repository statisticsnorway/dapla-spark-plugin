package no.ssb.dapla.spark.plugin.token;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;

import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;

// TODO: We could do better:
//  https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/SparkSessionExtensions.html
//  https://issues.apache.org/jira/browse/SPARK-24918
//  https://spark.apache.org/docs/2.4.5/api/java/org/apache/spark/scheduler/SparkListener.html
public class SparkConfStore implements TokenStore {

    private static final Logger log = LoggerFactory.getLogger(SparkConfStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    static SparkConfStore INSTANCE;

    static {
        try {
            get();
        } catch (Exception ex) {
            log.warn("Failed to load config from default session", ex);
        }
    }

    private final SparkConf conf;

    public SparkConfStore(SparkConf conf) {
        this.conf = Objects.requireNonNull(conf);
    }

    public static synchronized SparkConfStore get() {
        // Can happen if the static block fails.
        if (INSTANCE == null) {
            SparkSession sparkSession = SparkSession.getActiveSession().get();
            SparkContext currentContext = sparkSession.sparkContext();
            SparkConf conf = currentContext.getConf();
            INSTANCE = new SparkConfStore(conf);
        }
        return INSTANCE;
    }

    private static String extractClientSecret(String credentialsFile) {
        try {
            JsonNode credentialsJson = MAPPER.readTree(Paths.get(credentialsFile).toFile());
            return Objects.requireNonNull(credentialsJson.get("client_secret"),
                    String.format("Cannot find 'client_secret' in credentials file %s", credentialsFile)).asText();
        } catch (IOException e) {
            throw new RuntimeException("Error accessing credentials file: " + credentialsFile, e);
        }
    }

    private static String extractClientId(String credentialsFile) {
        try {
            JsonNode credentialsJson = MAPPER.readTree(Paths.get(credentialsFile).toFile());
            return Objects.requireNonNull(credentialsJson.get("client_id"),
                    String.format("Cannot find 'client_id' in credentials file %s", credentialsFile)).asText();
        } catch (IOException e) {
            throw new RuntimeException("Error accessing credentials file: " + credentialsFile, e);
        }
    }

    @Override
    public Optional<String> getClientId() {
        String credentialFile = conf.get(CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE, "");
        if (credentialFile.isEmpty()) {
            return Optional.ofNullable(conf.get(CONFIG_ROUTER_OAUTH_CLIENT_ID, null));
        } else {
            return Optional.of(extractClientId(credentialFile));
        }
    }

    public void setClientId(String clientId) {
        conf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, clientId);
    }

    @Override
    public Optional<String> getClientSecret() {
        String credentialFile = conf.get(CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE, "");
        if (credentialFile.isEmpty()) {
            return Optional.ofNullable(conf.get(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, null));
        } else {
            return Optional.of(extractClientSecret(credentialFile));
        }
    }

    public void setClientSecret(String clientSecret) {
        conf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, clientSecret);
    }

    @Override
    public String getTokenUrl() {
        return Objects.requireNonNull(conf.get(CONFIG_ROUTER_OAUTH_TOKEN_URL));
    }

    public void setTokenUrl(String tokenUrl) {
        conf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, tokenUrl);
    }

    public void setTokenUrl(HttpUrl tokenUrl) {
        conf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, tokenUrl.toString());
    }

    @Override
    public String getAccessToken() {
        return Objects.requireNonNull(conf.get(SPARK_SSB_ACCESS_TOKEN, null), "missing access token");
    }

    @Override
    public String getRefreshToken() {
        return Objects.requireNonNull(conf.get(SPARK_SSB_REFRESH_TOKEN, null), "missing refresh token");
    }

    @Override
    public void putAccessToken(String access) {
        conf.set(SPARK_SSB_ACCESS_TOKEN, Objects.requireNonNull(access, "access token cannot be null"));
    }

    @Override
    public void putRefreshToken(String refresh) {
        conf.set(SPARK_SSB_REFRESH_TOKEN, Objects.requireNonNull(refresh, "refresh token cannot be null"));
    }
}
