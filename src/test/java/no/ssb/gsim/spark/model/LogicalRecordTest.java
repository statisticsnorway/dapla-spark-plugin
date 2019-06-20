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


public class LogicalRecordTest {

    private String exampleDataset;
    private MockWebServer server;

    @Before
    public void setUp() throws Exception {
        Request getExampleDataset = new Request.Builder()
                .url("https://github.com/statisticsnorway/gsim-raml-schema/raw/master/examples/" +
                        "_main/LogicalRecord_Person_1.json")
                .build();

        try (Response execute = new OkHttpClient().newCall(getExampleDataset).execute()) {
            exampleDataset = execute.body().string();
        }

        server = new MockWebServer();
    }

    @Test
    public void testDeserialize() throws IOException {
        server.enqueue(new MockResponse().setBody(exampleDataset));
        server.start();
        HttpUrl baseUrl = server.url("/test/");
        LogicalRecord.Fetcher fetcher = new LogicalRecord.Fetcher();
        fetcher.withPrefix(baseUrl);
        fetcher.withClient(new OkHttpClient());
        fetcher.withMapper(new ObjectMapper());

        LogicalRecord logicalRecord = fetcher.fetch("unitDatasetId");
        assertThat(logicalRecord).isNotNull();
        assertThat(logicalRecord.getInstanceVariables()).containsExactly(
                "/InstanceVariable/09c24ccd-6765-4d8c-84ae-b4c7d55ad4ea",
                "/InstanceVariable/f908b24d-e5c4-4954-bcab-02a2e4724af3",
                "/InstanceVariable/71aec966-eb44-4a49-9bbf-0606563ca0cb",
                "/InstanceVariable/930607c9-88c7-45a2-80b4-4532c8a66a4e",
                "/InstanceVariable/beb729fc-f418-46b1-bab1-8ed20d82b907",
                "/InstanceVariable/8d51f269-4216-4351-94e5-94602e704694"
        );

        assertThat(logicalRecord.getId()).isEqualTo("f1b61f07-c403-43c5-80db-2a1bfdd1bb37");
        assertThat(logicalRecord.getShortName()).isEqualTo("PERSON");

        logicalRecord.setId("blabla");
        String s = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(logicalRecord);
        System.out.println(s);
    }

    @Test
    public void testSerialize() {
    }
}