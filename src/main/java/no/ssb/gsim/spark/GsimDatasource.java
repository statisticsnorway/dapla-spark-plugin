package no.ssb.gsim.spark;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import scala.Option;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class GsimDatasource implements RelationProvider {

    public static final String OPENID_DISCOVERY =
            "https://keycloak.staging.ssbmod.net/auth/realms/ssb/protocol/openid-connect/token";
    private static final String PATH = "path";
    private final OkHttpClient client;

    public GsimDatasource() {

        OAuth2Interceptor oAuth2Interceptor = new OAuth2Interceptor(
                OPENID_DISCOVERY, "lds-postgres-gsim", "api-user-3", "890e9e58-b1b5-4705-a557-69c19c89dbcf"
        );
        this.client = new OkHttpClient.Builder().addInterceptor(oAuth2Interceptor).build();

        Request request = new Request.Builder().get()
                .url("https://lds.staging.ssbmod.net/ns/UnitDataSet/b9c10b86-5867-4270-b56e-ee7439fe381e")
                .build();
        try (Response response = client.newCall(request).execute()) {
            System.out.println(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {

        Option<String> pathOption = parameters.get(PATH);
        if (pathOption.isEmpty()) {
            throw new RuntimeException("'path' must be set");
        }

        // Validate the path.
        URI pathUri = URI.create(pathOption.get());

        URL ldsHost = null;
        try {
            ldsHost = new URL("https://lds-client.staging.ssbmod.net/be/lds-c/ns/");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        List<URI> uris = fetchDataUris(ldsHost, pathUri);

        ArrayList<URI> actualPaths = new ArrayList<>();
        actualPaths.add(URI.create("file:///home/hadrien/Projects/SSB/java-vtl-connectors/java-vtl-connectors-parquet/8986396a-be56-434e-8491-fd1148d8c2b9.parquet"));
        actualPaths.add(URI.create("file:///home/hadrien/Projects/SSB/java-vtl-connectors/java-vtl-connectors-parquet/8986396a-be56-434e-8491-fd1148d8c2b9.parquet"));

        return new GsimRelation(sqlContext, actualPaths);
    }

    private List<URI> fetchDataUris(URL ldsUrl, URI pathUri) {
        //new Request.Builder().url()
        return null;
    }
}
