package no.ssb.dapla.spark.sql;

import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.spark.catalog.CustomSparkCatalog;
import no.ssb.dapla.spark.catalog.TableCatalogIdentifier;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TableReader {

    private final SparkSession session;

    private TableReader(SparkSession session) {
        this.session = session;
    }

    public static TableReader forSession(SparkSession session) {
        return new TableReader(session);
    }

    public Dataset<Row> readFrom(String path) {
        ReadLocationResponse readLocation = getReadLocation(session.sparkContext().conf(), path);
        if (!readLocation.getAccessAllowed()) {
            throw new RuntimeException("Permission denied");
        }
        session.conf().set(SparkOptions.ACCESS_TOKEN, readLocation.getAccessToken());
        session.conf().set(SparkOptions.ACCESS_TOKEN_EXP, String.valueOf(readLocation.getExpirationTime()));
        SparkSession.getActiveSession().get().conf().set(SparkOptions.ACCESS_TOKEN, readLocation.getAccessToken());
        SparkSession.getActiveSession().get().conf().set(SparkOptions.ACCESS_TOKEN_EXP, String.valueOf(readLocation.getExpirationTime()));

        TableCatalogIdentifier identifier = TableCatalogIdentifier.fromPath(path);
        return session.sqlContext().table(identifier.toCatalogPath(CustomSparkCatalog.DEFAULT_CATALOG_NAME));
    }

    private static ReadLocationResponse getReadLocation(SparkConf sparkConf, String path) {
        DataAccessClient dataAccessClient = new DataAccessClient(sparkConf);

        return dataAccessClient.readLocation2(ReadLocationRequest.newBuilder()
                .setPath(path)
                .setSnapshot(0) // 0 means latest
                .build());
    }
}
