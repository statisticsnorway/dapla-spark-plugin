package no.ssb.dapla.spark.plugin;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static no.ssb.dapla.service.CatalogClient.*;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;
import static org.assertj.core.api.Assertions.*;

public class CatalogRelationTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private MockWebServer catalogMockServer;

    @Before
    public void setUp() throws IOException {
        this.catalogMockServer = new MockWebServer();
        this.catalogMockServer.start();
        HttpUrl baseUrl = catalogMockServer.url("/catalog/");

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceLocalFSTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .config(CONFIG_CATALOG_URL, baseUrl.toString())
                .config(CONFIG_ROUTER_OAUTH_TOKEN_URL, "http://localhost") // not used
                .config(CONFIG_ROUTER_OAUTH_CLIENT_ID, "na")
                .config(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, "na")
                .config(CONFIG_ROUTER_OAUTH_TOKEN_IGNORE_EXPIRY, "true")
                .config("spark.ssb.access", JWT.create().withClaim("preferred_username", "john")
                        .withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.HOURS)))
                        .sign(Algorithm.HMAC256("secret")))
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sqlContext = session.sqlContext();
    }

    @Test
    public void testListByPrefix() {
        catalogMockServer.enqueue(new MockResponse()
                .setBody("{\n" +
                        "  \"entries\": [{\n" +
                        "    \"path\": \"/skatt/person/test1\",\n" +
                        "    \"timestamp\": \"1585256968006\"\n" +
                        "  }, {\n" +
                        "    \"path\": \"/skatt/person/test2\",\n" +
                        "    \"timestamp\": \"1582719098762\"\n" +
                        "  }]\n" +
                        "}\n")
                .setResponseCode(200));

        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
                .load("/skatt/person/*");

        assertThat(dataset).isNotNull();
        dataset.printSchema();
        dataset.show();
        assertThat(dataset.count()).isEqualTo(2);
    }
}
