package no.ssb.dapla.service;


import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SaveMode;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;

import static no.ssb.dapla.service.SparkServiceClient.*;

public class SparkServiceClientStagingTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_ROUTER_URL, "https://dapla-spark.staging-bip-app.ssb.no/");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "https://keycloak.staging-bip-app.ssb.no/auth/realms/ssb/protocol/openid-connect/token");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_GRANT_TYPE, "client_credential");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_ID));
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_SECRET));
    }

    @Test
    @Ignore
    public void testRead() {
        SparkServiceClient sparkServiceClient = new SparkServiceClient(this.sparkConf);

        for (Tuple2<String, String> tuple : this.sparkConf.getAll()) {
            System.out.println(tuple._1);
            System.out.println(tuple._2);
        }
        Dataset dataset = sparkServiceClient.getDataset("rune.lind@ssbmod.net", "skatt.person.2019.inndata.mytestdataset");
        System.out.println(dataset);
    }

    @Test
    @Ignore
    public void testWrite() {
        SparkServiceClient sparkServiceClient = new SparkServiceClient(this.sparkConf);

        final Dataset.Valuation valuation = Dataset.Valuation.SHIELDED;
        String state = "INPUT";

        final String namespace = "skatt.person.2019.inndata.mytestdataset";

        Dataset dataset = sparkServiceClient.createDataset("rune.lind@ssbmod.net",
                SaveMode.Overwrite, namespace, valuation.name(), state);
        final String dataId = "gs://dev-datalager-store/" + namespace + "/" + dataset.getId().getId();

        dataset = no.ssb.dapla.catalog.protobuf.Dataset.newBuilder().mergeFrom(dataset)
                .setId(DatasetId.newBuilder().setId(dataset.getId().getId()).addName(namespace).build())
                .setValuation(valuation)
                .setState(no.ssb.dapla.catalog.protobuf.Dataset.DatasetState.valueOf(state))
                .clearLocations()
                .addLocations(dataId).build();

        sparkServiceClient.writeDataset(dataset, "user1");

        System.out.println(dataset);
    }
}