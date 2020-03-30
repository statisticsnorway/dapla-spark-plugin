package no.ssb.dapla.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static no.ssb.dapla.service.CatalogClient.*;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;
import static org.assertj.core.api.Assertions.*;

public class CatalogClientTest {

    private SparkConf sparkConf = new SparkConf();
    private MockWebServer server;

    @Before
    public void setUp() throws IOException {
        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/catalog/");
        this.sparkConf.set(CONFIG_CATALOG_URL, baseUrl.toString());
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "http://localhost"); // not used
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, "na");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, "na");
        this.sparkConf.set("spark.ssb.access", JWT.create().withClaim("preferred_username", "johndoe")
                .withExpiresAt(Date.from(Instant.now().plus(1, ChronoUnit.HOURS))).sign(Algorithm.HMAC256("secret")));
    }

    @Test
    public void testListByPrefix() {
        server.enqueue(new MockResponse()
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
        CatalogClient dataAccessClient = new CatalogClient(this.sparkConf);
        ListByPrefixResponse response = dataAccessClient.listByPrefix(ListByPrefixRequest.newBuilder()
                .setPrefix("/skatt/person/")
                .build());
        assertThat(response.getEntriesCount()).isEqualTo(2);
    }

}