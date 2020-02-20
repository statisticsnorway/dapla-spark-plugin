package no.ssb.dapla.service;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.AccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import org.apache.spark.SparkConf;

import java.net.URI;
import java.net.URISyntaxException;

public class DataAccessGRPCClient {

    static final String CONFIG = "spark.ssb.dapla.";
    static final String CONFIG_ROUTER_URL = CONFIG + "router.url";

    private final DataAccessServiceGrpc.DataAccessServiceBlockingStub dataAccessServiceBlockingStub;

    public DataAccessGRPCClient(final SparkConf conf) {
        this(getUri(conf));
    }

    public DataAccessGRPCClient(final URI uri) {
        this(uri.getHost(), uri.getPort());
    }

    public DataAccessGRPCClient(String host, int port) {
        Channel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        dataAccessServiceBlockingStub = DataAccessServiceGrpc.newBlockingStub(channel);
    }

    public AccessTokenResponse getTokenResponse() {
        AccessTokenRequest tokenRequest = AccessTokenRequest.newBuilder()
                .setUserId("user")
                .setPath("path")
                .setPrivilege(AccessTokenRequest.Privilege.READ)
                .build();

        return dataAccessServiceBlockingStub.getAccessToken(tokenRequest);
    }

    private static URI getUri(SparkConf conf) {
        URI uri;
        try {
            uri = new URI(conf.get(CONFIG_ROUTER_URL));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        return uri;
    }
}
