package no.ssb.dapla.spark.router;


import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.dapla.spark.plugin.OAuth2Interceptor;
import no.ssb.lds.gsim.okhttp.api.Client;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class routes calls from spark plugin to remote services.
 */
public class SparkServiceRouter {

    private final String bucket;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CONFIG = "spark.ssb.dapla.";
    private static final String CONFIG_ROUTER_URL = CONFIG + "router.url";
    private static final String CONFIG_ROUTER_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    private static final String CONFIG_ROUTER_OAUTH_GRANT_TYPE = CONFIG + "oauth.grantType";
    private static final String CONFIG_ROUTER_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    private static final String CONFIG_ROUTER_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";
    private static final String CONFIG_ROUTER_OAUTH_USER_NAME = CONFIG + "oauth.userName";
    private static final String CONFIG_ROUTER_OAUTH_PASSWORD = CONFIG + "oauth.password";

    // Mock access rules from userId and namespace
    private Map<String, Set<String>> authMock = new ConcurrentHashMap<>();

    // Mock a mapping from namespace to dataset-locations
    private Map<String, DataLocation> catalogMock = new ConcurrentHashMap<>();

    public SparkServiceRouter(String bucket) {
        this.bucket = bucket;
    }

    private static SparkServiceRouter instance;

    public static SparkServiceRouter getInstance(String bucket) {
        if (instance == null) {
            instance = new SparkServiceRouter(bucket);
        }
        return instance;
    }

    public DataLocation read(String userId, String namespace) {
        System.out.println("Brukernavn: " + userId);
        if (authMock.get(userId) == null || !authMock.get(userId).contains(namespace)) {
            throw new RuntimeException(String.format("Din bruker %s har ikke tilgang til %s", userId, namespace));
        }
        DataLocation location = catalogMock.get(namespace);
        if (location != null) {
            System.out.println("Fant følgende datasett: " + StringUtils.join(location.getPaths().stream().map(URI::toString).toArray(), ", "));
        } else {
            System.out.println("Fant ingen datasett");
        }
        return location;
    }

    public DataLocation write(SaveMode mode, String userId, String namespace, String dataId) {
        try {
            System.out.println("Oppretter datasett: " + dataId);
            final DataLocation dataLocation = new DataLocation(namespace, bucket, Arrays.asList(new URI(dataId)));
            if (authMock.get(userId) == null) {
                authMock.put(userId, Collections.singleton(namespace));
            } else {
                Set<String> namespaces = new HashSet<>(authMock.get(userId));
                namespaces.add(namespace);
                authMock.put(userId, namespaces);
            }
            if (mode == SaveMode.Overwrite) {
                catalogMock.put(namespace, dataLocation);
                return dataLocation;
            } else if (mode == SaveMode.Append) {
                DataLocation dl = catalogMock.get(namespace);
                if (dl != null) {
                    dl.getPaths().add(new URI(dataId));
                } else {
                    catalogMock.put(namespace, dataLocation);
                    dl = dataLocation;
                }
                return dl;
            } else {
                return catalogMock.get(namespace);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private Client createSparkServiceClient(final SparkConf conf) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        return new Client()
                .withClient(builder.build())
                .withMapper(MAPPER)
                .withPrefix(HttpUrl.parse(conf.get(CONFIG_ROUTER_URL)));
    }

    private Optional<OAuth2Interceptor> createOAuth2Interceptor(final SparkConf conf) {
        if (conf.contains(CONFIG_ROUTER_OAUTH_TOKEN_URL)) {
            OAuth2Interceptor interceptor = new OAuth2Interceptor(
                    conf.get(CONFIG_ROUTER_OAUTH_TOKEN_URL, null),
                    OAuth2Interceptor.GrantType.valueOf(
                            conf.get(CONFIG_ROUTER_OAUTH_GRANT_TYPE).toUpperCase()
                    ),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_ID, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, null),
                    conf.get(CONFIG_ROUTER_OAUTH_USER_NAME, null),
                    conf.get(CONFIG_ROUTER_OAUTH_PASSWORD, null)
            );
            return Optional.of(interceptor);
        }
        return Optional.empty();
    }

}
