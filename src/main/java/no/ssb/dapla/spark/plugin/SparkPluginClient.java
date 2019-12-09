package no.ssb.dapla.spark.plugin;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import no.ssb.dapla.spark.protobuf.HelloRequest;
import no.ssb.dapla.spark.protobuf.HelloResponse;
import no.ssb.dapla.spark.protobuf.SparkPluginServiceGrpc;

public class SparkPluginClient {

    private SparkPluginServiceGrpc.SparkPluginServiceBlockingStub pluginClient;

    public SparkPluginClient() {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress("host.docker.internal", 8092);
        channelBuilder.usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        pluginClient = SparkPluginServiceGrpc.newBlockingStub(channel);
    }

    void call(String message) {
        HelloResponse helloFromClient = pluginClient.sayHello(HelloRequest.newBuilder().setGreeting(message).build());

        System.out.println(helloFromClient.getReply());
    }

}
