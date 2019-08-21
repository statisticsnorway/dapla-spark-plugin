package no.ssb.gsim.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.gsim.spark.model.UnitDataset;
import no.ssb.gsim.spark.model.api.Client;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import scala.Option;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class GsimDatasource implements RelationProvider, CreatableRelationProvider {

    private static final String CONFIG = "spark.ssb.gsim.";

    // The default location used when writing data.
    static final String CONFIG_LOCATION_PREFIX = CONFIG + "location";

    // The lds url to use. Required.
    static final String CONFIG_LDS_URL = CONFIG + "ldsUrl";

    // oAuth parameters. Must all be set to be used.
    private static final String CONFIG_LDS_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    private static final String CONFIG_LDS_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    private static final String CONFIG_LDS_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";
    private static final String CONFIG_LDS_OAUTH_USER_NAME = CONFIG + "oauth.userName";
    private static final String CONFIG_LDS_OAUTH_PASSWORD = CONFIG + "oauth.password";
    private static final String CONFIG_LDS_OAUTH_GRANT_TYPE = CONFIG + "oauth.grantType";

    private static final String PATH = "path";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Client createLdsClient(final SparkConf conf) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        return new Client()
                .withClient(builder.build())
                .withMapper(MAPPER)
                .withPrefix(HttpUrl.parse(conf.get(CONFIG_LDS_URL)));
    }

    private Optional<OAuth2Interceptor> createOAuth2Interceptor(final SparkConf conf) {
        if (conf.contains(CONFIG_LDS_OAUTH_TOKEN_URL)) {
            OAuth2Interceptor interceptor = new OAuth2Interceptor(
                    conf.get(CONFIG_LDS_OAUTH_TOKEN_URL),
                    OAuth2Interceptor.GrantType.valueOf(
                            conf.get(CONFIG_LDS_OAUTH_GRANT_TYPE)
                    ),
                    conf.get(CONFIG_LDS_OAUTH_CLIENT_ID),
                    conf.get(CONFIG_LDS_OAUTH_CLIENT_SECRET),
                    conf.get(CONFIG_LDS_OAUTH_USER_NAME),
                    conf.get(CONFIG_LDS_OAUTH_PASSWORD)
            );
            return Optional.of(interceptor);
        }
        return Optional.empty();
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        URI datasetUri = extractPath(parameters);
        String datasetId = extractDatasetId(datasetUri);
        Client ldsClient = createLdsClient(sqlContext.sparkContext().conf());
        UnitDataset dataset = ldsClient.fetchUnitDataset(datasetId, Instant.now()).join();
        List<URI> dataUris = extractUris(dataset);
        return new GsimRelation(sqlContext, dataUris);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {

        URI datasetUri = extractPath(parameters);
        String datasetId = extractDatasetId(datasetUri);
        Client ldsClient = createLdsClient(sqlContext.sparkContext().conf());
        UnitDataset dataset = ldsClient.fetchUnitDataset(datasetId, Instant.now()).join();
        List<URI> dataUris = extractUris(dataset);

        // Create a new path with the current time.
        try {
            String locationPrefix = sqlContext.getConf(CONFIG_LOCATION_PREFIX);
            URI prefixUri = URI.create(locationPrefix);
            URI newDataUri = new URI(
                    prefixUri.getScheme(),
                    String.format("%s/%s/%d",
                            prefixUri.getSchemeSpecificPart(),
                            datasetUri.getSchemeSpecificPart(), System.currentTimeMillis()
                    ),
                    null
            );

            List<URI> newDataUris;
            if (mode.equals(SaveMode.Overwrite)) {
                newDataUris = Collections.singletonList(newDataUri);
            } else if (mode.equals(SaveMode.Append)) {
                newDataUris = new ArrayList<>();
                newDataUris.add(newDataUri);
                newDataUris.addAll(dataUris);
            } else {
                throw new IllegalArgumentException("Unsupported mode " + mode);
            }

            dataset.setDataSourcePath(newDataUris.stream().map(URI::toASCIIString).collect(Collectors.joining(",")));
            data.coalesce(1).write().parquet(newDataUri.toASCIIString());

            ldsClient.updateUnitDataset(datasetId, dataset).join();
            return createRelation(sqlContext, parameters);
        } catch (URISyntaxException e) {
            throw new RuntimeException("could not generate new file uri", e);
        } catch (IOException e) {
            throw new RuntimeException("could not update lds", e);
        }
    }

    private List<URI> extractUris(UnitDataset dataset) {
        // Find all the files for the dataset.
        List<String> dataSources = Arrays.asList(dataset.getDataSourcePath().split(","));
        return dataSources.stream().map(URI::create).collect(Collectors.toList());
    }

    private URI extractPath(Map<String, String> parameters) {
        Option<String> pathOption = parameters.get(PATH);
        if (pathOption.isEmpty()) {
            throw new RuntimeException("'path' must be set");
        }
        return URI.create(pathOption.get());
    }

    private String extractDatasetId(URI pathUri) {
        List<String> schemes = Arrays.asList(pathUri.getScheme().split("\\+"));
        if (!schemes.contains("lds") || !schemes.contains("gsim")) {
            throw new IllegalArgumentException("invalid scheme. Please use lds+gsim://[port[:post]]/path");
        }

        String path = pathUri.getPath();
        if (path == null || path.isEmpty()) {
            // No path. Use the host as id.
            path = pathUri.getHost();
        }
        return path;
    }

    public static class Configuration {
        private String location;

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }
}
