package no.ssb.dapla.spark.sql;

import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.spark.catalog.TableCatalogIdentifier;
import no.ssb.dapla.spark.plugin.SparkOptions;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.CreateTableWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class TableWriter {

    private static final String METADATA_FOLDER = ".metadata";
    private final Dataset<Row> dataset;
    private final Map<String, String> props = new HashMap<>();

    private TableWriter(Dataset<Row> dataset) {
        this.dataset = dataset;
    }

    public static TableWriter forDataset(Dataset<Row> dataset) {
        return new TableWriter(dataset);
    }

    public TableWriter property(String key, String value) {
        this.props.put(key, value);
        return this;
    }

    public CreateTableWriter<Row> writeTo(String path) {
        TableCatalogIdentifier identifier = TableCatalogIdentifier.fromPath(path);
        Valuation valuation = Valuation.valueOf(props.get("valuation"));
        DatasetState state = DatasetState.valueOf(props.get("state"));

        WriteLocationResponse writeLocation = getWriteLocation(dataset.sparkSession().sparkContext().conf(), path, valuation, state);
        if (!writeLocation.getAccessAllowed()) {
            throw new RuntimeException("Permission denied");
        }
        String dataPath = String.format("%s%s", writeLocation.getParentUri(), path);
        String metadataPath = String.format("%s%s/%s", writeLocation.getParentUri(), path, METADATA_FOLDER);
        SparkSession.getActiveSession().get().conf().set(SparkOptions.ACCESS_TOKEN, writeLocation.getAccessToken());
        SparkSession.getActiveSession().get().conf().set(SparkOptions.ACCESS_TOKEN_EXP, String.valueOf(writeLocation.getExpirationTime()));

        return dataset.writeTo(identifier.toCatalogPath("ssb_catalog"))
                .using("iceberg")
                .tableProperty("valuation", valuation.name())
                .tableProperty("state", state.name())
                .tableProperty("write.folder-storage.path", dataPath)
                .tableProperty("write.metadata.path", metadataPath);
    }

    private static WriteLocationResponse getWriteLocation(SparkConf sparkConf, String path, Valuation valuation, DatasetState state) {
        DataAccessClient dataAccessClient = new DataAccessClient(sparkConf);

        return dataAccessClient.writeLocation(WriteLocationRequest.newBuilder()
                .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                        .setId(DatasetId.newBuilder()
                                .setPath(path)
                                .setVersion("0")
                                .build())
                        .setType(Type.BOUNDED)
                        .setValuation(valuation)
                        .setState(state)
                        .build()))
                .build());
    }
}
