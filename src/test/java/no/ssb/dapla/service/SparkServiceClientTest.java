package no.ssb.dapla.service;


import no.ssb.dapla.catalog.protobuf.Dataset;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static no.ssb.dapla.service.SparkServiceClient.*;
import static org.assertj.core.api.Assertions.*;

public class SparkServiceClientTest {

    private SparkConf sparkConf = new SparkConf();
    private MockWebServer server;

    @Before
    public void setUp() throws IOException {
        this.server = new MockWebServer();
        this.server.start();
        HttpUrl baseUrl = server.url("/spark-service/");
        this.sparkConf.set(CONFIG_ROUTER_URL, baseUrl.toString());
    }

    @Test
    public void testRead() throws IOException, InterruptedException {
        InputStream in = this.getClass().getResourceAsStream("data/dataset.json");
        String mockResult = new Buffer().readFrom(in).readByteString().utf8();
        server.enqueue(new MockResponse().setBody(mockResult).setResponseCode(200));
        SparkServiceClient sparkServiceClient = new SparkServiceClient(this.sparkConf);

        Dataset dataset = sparkServiceClient.getDataset("rune.lind@ssbmod.net", "skatt.person.mytestdataset");
        assertThat(dataset.getId().getName(0)).isEqualTo("skatt.person.mytestdataset");

        RecordedRequest recordedRequest = server.takeRequest();
        HttpUrl requestUrl = recordedRequest.getRequestUrl();
        assertThat(requestUrl.query()).isEqualTo("name=skatt.person.mytestdataset&operation=READ&userId=rune.lind@ssbmod.net");
    }
}