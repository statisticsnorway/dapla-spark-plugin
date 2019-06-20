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


public class InstanceVariableTest {

    private String exampleBody;
    private MockWebServer server;

    @Before
    public void setUp() throws Exception {
        Request getExampleDataset = new Request.Builder()
                .url("https://github.com/statisticsnorway/gsim-raml-schema/raw/master/examples/" +
                        "_main/InstanceVariable_PersonIncome.json")
                .build();

        try (Response execute = new OkHttpClient().newCall(getExampleDataset).execute()) {
            exampleBody = execute.body().string();
        }

        server = new MockWebServer();
    }

    @Test
    public void testDeserialize() throws IOException {
        server.enqueue(new MockResponse().setBody(exampleBody));
        server.start();
        HttpUrl baseUrl = server.url("/test/");
        InstanceVariable.Fetcher fetcher = new InstanceVariable.Fetcher();
        fetcher.withPrefix(baseUrl);
        fetcher.withClient(new OkHttpClient());
        fetcher.withMapper(new ObjectMapper());

        InstanceVariable instanceVariable = fetcher.fetch("unitDatasetId");
        assertThat(instanceVariable).isNotNull();
        assertThat(instanceVariable.getShortName()).isEqualTo("INCOME");
        assertThat(instanceVariable.getDataStructureComponentType()).isEqualTo("MEASURE");


        assertThat(instanceVariable.getId()).isEqualTo("71aec966-eb44-4a49-9bbf-0606563ca0cb");
        assertThat(instanceVariable.getShortName()).isEqualTo("INCOME");

        instanceVariable.setId("blabla");
        String s = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(instanceVariable);
        System.out.println(s);
    }

    @Test
    public void testSerialize() {
    }
}