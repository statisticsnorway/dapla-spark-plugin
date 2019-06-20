package no.ssb.gsim.spark.model;


import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;


public class UnitDatastructureTest {

    private String exampleDataStructure;
    private MockWebServer server;

    @Before
    public void setUp() throws Exception {
        Request getExampleDataset = new Request.Builder()
                .url("https://github.com/statisticsnorway/gsim-raml-schema/raw/master/examples/" +
                        "_main/UnitDataStructure_Person_1.json")
                .build();

        try (Response execute = new OkHttpClient().newCall(getExampleDataset).execute()) {
            exampleDataStructure = execute.body().string();
        }

        server = new MockWebServer();
    }

    @Test
    public void testDeserialize() throws IOException {
        server.enqueue(new MockResponse().setBody(exampleDataStructure));
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