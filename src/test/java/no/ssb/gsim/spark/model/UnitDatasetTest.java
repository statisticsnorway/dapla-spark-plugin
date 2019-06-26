package no.ssb.gsim.spark.model;


import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.Buffer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;


public class UnitDatasetTest {

    private String exampleDataset;
    private MockWebServer server;

    @Before
    public void setUp() throws Exception {
        Request getExampleDataset = new Request.Builder()
                .url("https://raw.githubusercontent.com/statisticsnorway/" +
                        "gsim-raml-schema/master/examples/_main/UnitDataSet_Person_1.json")
                .build();

        try (Response execute = new OkHttpClient().newCall(getExampleDataset).execute()) {
            exampleDataset = execute.body().string();
        }

        server = new MockWebServer();
    }

    @Test
    public void testDeserialize() throws IOException {
        InputStream in = this.getClass().getResourceAsStream(
                "/no/ssb/gsim/spark/data/UnitDataSet_Person_1.json");
        server.enqueue(new MockResponse().setBody(new Buffer().readFrom(in)));
        server.start();
        HttpUrl baseUrl = server.url("/test/");
        UnitDataset.Fetcher fetcher = new UnitDataset.Fetcher();
        fetcher.withPrefix(baseUrl);
        fetcher.withClient(new OkHttpClient());
        fetcher.withMapper(new ObjectMapper());

        UnitDataset unitDataset = fetcher.fetch("unitDatasetId");
        assertThat(unitDataset).isNotNull();
        assertThat(unitDataset.getDataSourcePath()).isEqualTo("%DATA_PATH%");
        assertThat(unitDataset.getId()).isEqualTo("b9c10b86-5867-4270-b56e-ee7439fe381e");

        unitDataset.setId("blabla");
        String s = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(unitDataset);
        System.out.println(s);
    }

    @Test
    public void testSerialize() {
    }
}