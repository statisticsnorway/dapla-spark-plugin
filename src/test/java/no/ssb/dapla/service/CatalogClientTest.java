package no.ssb.dapla.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.protobuf.ByteString;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.catalog.protobuf.SignedDataset;
import no.ssb.dapla.dataset.api.DatasetMetaAll;
import no.ssb.dapla.utils.ProtobufJsonUtils;
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

import static no.ssb.dapla.service.CatalogClient.CONFIG_CATALOG_URL;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_CLIENT_ID;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_CLIENT_SECRET;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.CONFIG_ROUTER_OAUTH_TOKEN_URL;
import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void testWriteSignedDataset() {
        server.enqueue(new MockResponse().setResponseCode(200));
        CatalogClient catalogClient = new CatalogClient(this.sparkConf);

        catalogClient.writeDataset(SignedDataset.newBuilder()
                .setDatasetMetaAllBytes(ByteString.copyFromUtf8(ProtobufJsonUtils.toString(DatasetMetaAll.newBuilder()
                        .setId(no.ssb.dapla.dataset.api.DatasetId.newBuilder().setPath("1").build())
                        .setValuation(no.ssb.dapla.dataset.api.Valuation.SHIELDED)
                        .setState(no.ssb.dapla.dataset.api.DatasetState.OUTPUT)
                        .setRandom("398ghaD")
                        .setParentUri("f1")
                        .build())))
                .build());
    }

}