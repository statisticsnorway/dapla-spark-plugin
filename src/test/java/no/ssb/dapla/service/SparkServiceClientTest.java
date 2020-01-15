package no.ssb.dapla.service;


import no.ssb.dapla.catalog.protobuf.Dataset;
import org.junit.Test;

public class SparkServiceClientTest {

    @Test
    public void test() {
        SparkServiceClient sparkServiceClient = new SparkServiceClient();

        Dataset dataset = sparkServiceClient.getDataset();

        System.out.println(dataset.getId());
    }
}