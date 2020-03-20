package no.ssb.dapla.service;

import com.google.protobuf.ByteString;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static no.ssb.dapla.service.DataAccessClient.CONFIG_DATA_ACCESS_URL;
import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;

public class DataAccessClientStagingTest {

    private SparkConf sparkConf = new SparkConf();

    @Before
    public void setUp() {
        this.sparkConf.set(CONFIG_DATA_ACCESS_URL, "https://data-access.staging-bip-app.ssb.no/");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_TOKEN_URL, "https://keycloak.staging-bip-app.ssb.no/auth/realms/ssb/protocol/openid-connect/token");
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_ID, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_ID));
        this.sparkConf.set(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, System.getenv(CONFIG_ROUTER_OAUTH_CLIENT_SECRET));
        this.sparkConf.set(SPARK_SSB_ACCESS_TOKEN, System.getenv("spark.ssb.access"));
        this.sparkConf.set(SPARK_SSB_REFRESH_TOKEN, System.getenv("spark.ssb.refresh"));
    }

    @Test
    @Ignore
    public void testGetReadLocation() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/tmp/bjorn-andre.skaar@ssbmod.net/test")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
        System.out.println(readLocationResponse.getAccessAllowed());
        System.out.println(readLocationResponse.getParentUri());
    }

    @Test
    @Ignore
    public void testGetWriteLocation() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        WriteLocationResponse writeLocationResponse = dataAccessClient.writeLocation(WriteLocationRequest.newBuilder()
                .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                        .setId(DatasetId.newBuilder()
                                .setPath("/tmp/bjorn-andre.skaar@ssbmod.net/test")
                                .setVersion(1000)
                                .build())
                        .setType(DatasetMeta.Type.BOUNDED)
                        .setValuation(DatasetMeta.Valuation.INTERNAL)
                        .setState(DatasetMeta.DatasetState.INPUT)
                        .build()))
                .build());

        System.out.println(writeLocationResponse.getAccessAllowed());
        String metadataJson = writeLocationResponse.getValidMetadataJson().toStringUtf8();

        DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(metadataJson, DatasetMeta.class);
        DatasetUri pathToNewDataSet = DatasetUri.of(datasetMeta.getParentUri(), datasetMeta.getId().getPath(), datasetMeta.getId().getVersion());
        System.out.println("Path to new dataset " + pathToNewDataSet);

        WriteAccessTokenResponse writeAccessTokenResponse = dataAccessClient.writeAccessToken(WriteAccessTokenRequest.newBuilder()
                .setMetadataJson(ByteString.copyFromUtf8(metadataJson))
                .setMetadataSignature(writeLocationResponse.getMetadataSignature())
                .build());

        System.out.println(writeAccessTokenResponse.getAccessToken());
    }

    @Test
    @Ignore
    public void testGetAccessToken() {
        DataAccessClient dataAccessClient = new DataAccessClient(this.sparkConf);
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath("/tmp/bjorn-andre.skaar@ssbmod.net/test")
                .setSnapshot(0) // 0 means resolve to latest version
                .build());
        ReadAccessTokenResponse readAccessTokenResponse = dataAccessClient.readAccessToken(ReadAccessTokenRequest.newBuilder()
                .setPath("/tmp/bjorn-andre.skaar@ssbmod.net/test")
                .setVersion(readLocationResponse.getVersion())
                .build());
        System.out.println(readAccessTokenResponse.getAccessToken());
    }
}