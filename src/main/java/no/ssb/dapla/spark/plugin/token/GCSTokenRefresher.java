package no.ssb.dapla.spark.plugin.token;

import io.opentracing.Span;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.spark.plugin.SparkOptions;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class GCSTokenRefresher {

    private final DataAccessClient dataAccessClient;
    private final String localPath;
    private final Span span;
    private Long lastRefresh = 0L;
    private final Long lastRefreshTheshold = 5 * 60 * 1000L;

    public GCSTokenRefresher(SparkConf sparkConf, String localPath, Span span) {
        this.dataAccessClient = new DataAccessClient(sparkConf, span);
        this.localPath = localPath;
        this.span = span;
    }

    public boolean shouldRefresh() {
        return System.currentTimeMillis() > lastRefresh + lastRefreshTheshold;
    }

    public ReadLocationResponse getReadLocation() {
        ReadLocationResponse readLocationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath(localPath)
                .setSnapshot(0) // 0 means latest
                .build());

        if (!readLocationResponse.getAccessAllowed()) {
            span.log("User got permission denied");
            throw new RuntimeException("Permission denied");
        }
        this.lastRefresh = System.currentTimeMillis();
        return readLocationResponse;
    }

    public WriteLocationResponse getWriteLocation(String version, Valuation valuation, DatasetState state) {
        WriteLocationResponse writeLocationResponse = dataAccessClient.writeLocation(WriteLocationRequest.newBuilder()
                .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                        .setId(DatasetId.newBuilder()
                                .setPath(localPath)
                                .setVersion(version)
                                .build())
                        .setType(Type.BOUNDED)
                        .setValuation(valuation)
                        .setState(state)
                        .build()))
                .build());

        if (!writeLocationResponse.getAccessAllowed()) {
            throw new RuntimeException("Permission denied");
        }
        this.lastRefresh = System.currentTimeMillis();
        return writeLocationResponse;
    }


    public static void setUserContext(SparkSession sparkSession, String accessToken, long expirationTime) {
        if (sparkSession.conf().contains(SparkOptions.ACCESS_TOKEN)) {
            System.out.println("Access token already exists");
        }
        if (accessToken != null) {
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN, accessToken);
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN_EXP, expirationTime);
        }
    }

    public static void unsetUserContext(SparkSession sparkSession) {
        sparkSession.conf().unset(SparkOptions.ACCESS_TOKEN);
        sparkSession.conf().unset(SparkOptions.ACCESS_TOKEN_EXP);
    }

}
