package no.ssb.gsim.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.gsim.spark.model.InstanceVariable;
import no.ssb.gsim.spark.model.LogicalRecord;
import no.ssb.gsim.spark.model.UnitDataStructure;
import no.ssb.gsim.spark.model.UnitDataset;
import no.ssb.gsim.spark.model.api.Client;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import scala.Option;
import scala.collection.immutable.Map;

import java.net.URI;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GsimDatasource implements RelationProvider {

    public static final String OPENID_DISCOVERY =
            "https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token";
    private static final String PATH = "path";
    private final OkHttpClient client;
    private final Client ldsClient;

    public GsimDatasource() {

        // TODO: Use env or spark/hadoop config.
        OAuth2Interceptor oAuth2Interceptor = new OAuth2Interceptor(
                OPENID_DISCOVERY,
                "lds-c-postgres-gsim",
                "api-user-3",
                "890e9e58-b1b5-4705-a557-69c19c89dbcf"
        );
        this.client = new OkHttpClient.Builder().addInterceptor(oAuth2Interceptor).build();

        // Builder style
        ldsClient = new Client().withClient(this.client).withMapper(new ObjectMapper());
        // setter style
        ldsClient.withPrefix(HttpUrl.parse("https://lds-c.staging.ssbmod.net/ns/"));
    }

    private void testFetch(UnitDataset unitDataset) {
        System.out.println("Datasource path" + unitDataset.getDataSourcePath().split(","));

        System.out.println(unitDataset);
        UnitDataStructure unitDataStructure = unitDataset.fetchUnitDataStructure().join();
        System.out.println(unitDataStructure);
        List<LogicalRecord> logicalRecords = unitDataStructure.fetchLogicalRecords().join();
        System.out.println(logicalRecords);
        for (LogicalRecord logicalRecord : logicalRecords) {
            List<InstanceVariable> instanceVariables = logicalRecord.fetchInstanceVariables().join();
            System.out.println(instanceVariables);
        }
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {

        Option<String> pathOption = parameters.get(PATH);
        if (pathOption.isEmpty()) {
            throw new RuntimeException("'path' must be set");
        }

        URI pathUri = URI.create(pathOption.get());
        List<URI> actualPaths = fetchDataUris(pathUri);
        return new GsimRelation(sqlContext, actualPaths);
    }

    private List<URI> fetchDataUris(URI pathUri) throws IllegalArgumentException {

        // Validate the scheme.
        List<String> schemes = Arrays.asList(pathUri.getScheme().split("\\+"));
        if (!schemes.contains("lds") || !schemes.contains("gsim")) {
            throw new IllegalArgumentException("invalid scheme. Please use lds+gsim://[port[:post]]/path");
        }

        String path = pathUri.getPath();
        if (path == null || path.isEmpty()) {
            // No path. Use the host as id.
            path = pathUri.getHost();
        }

        UnitDataset unitDataset = this.ldsClient.fetchUnitDataset(path, Instant.now()).join();

        // Find all the files for the dataset.
        List<String> dataSources = Arrays.asList(unitDataset.getDataSourcePath().split(","));
        return dataSources.stream().map(URI::create).collect(Collectors.toList());
    }
}
