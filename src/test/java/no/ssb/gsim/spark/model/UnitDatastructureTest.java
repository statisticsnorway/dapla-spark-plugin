package no.ssb.gsim.spark.model;


import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;


public class UnitDatastructureTest {

    private MockWebServer server;

    @Before
    public void setUp() {
        server = new MockWebServer();
    }

    @Test
    public void testDeserialize() throws IOException {
        InputStream in = this.getClass().getResourceAsStream(
                "/no/ssb/gsim/spark/data/UnitDataStructure_Person_1.json");
        server.enqueue(new MockResponse().setBody(new Buffer().readFrom(in)));
        server.start();
        HttpUrl baseUrl = server.url("/test/");
        UnitDataStructure.Fetcher fetcher = new UnitDataStructure.Fetcher();
        fetcher.withPrefix(baseUrl);
        fetcher.withClient(new OkHttpClient());
        fetcher.withMapper(new ObjectMapper());

        UnitDataStructure unitDataset = fetcher.fetch("unitDatasetId");
        assertThat(unitDataset).isNotNull();
        assertThat(unitDataset.getLogicalRecords()).containsExactly(
                "/LogicalRecord/f1b61f07-c403-43c5-80db-2a1bfdd1bb37"
        );
        assertThat(unitDataset.getId()).isEqualTo("cd3ecd46-c090-4bc6-80fb-db758eeb10fd");

        unitDataset.setId("blabla");
        String s = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(unitDataset);
        System.out.println(s);
    }

    @Test
    public void testSerialize() {
    }
}