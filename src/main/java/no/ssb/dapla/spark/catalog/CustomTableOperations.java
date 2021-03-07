package no.ssb.dapla.spark.catalog;

import no.ssb.dapla.catalog.protobuf.CatalogTable;
import no.ssb.dapla.catalog.protobuf.CreateTableRequest;
import no.ssb.dapla.catalog.protobuf.GetTableRequest;
import no.ssb.dapla.catalog.protobuf.GetTableResponse;
import no.ssb.dapla.catalog.protobuf.UpdateTableRequest;
import no.ssb.dapla.service.CatalogClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.spark.SparkConf;

public class CustomTableOperations extends BaseMetastoreTableOperations {
    private final TableIdentifier tableIdentifier;
    private final Configuration conf;
    private final FileIO fileIO;
    private final CatalogClient catalogClient;

    protected CustomTableOperations(SparkConf sparkConf, Configuration conf, TableIdentifier tableIdentifier) {
        this.conf = conf;
        this.conf.setBooleanIfUnset("fs.gs.impl.disable.cache",
                this.conf.getBoolean("spark.hadoop.fs.gs.impl.disable.cache", false));
        //this.fileIO = new CustomFileIO(this.conf);
        this.fileIO = new HadoopFileIO(this.conf);
        this.tableIdentifier = tableIdentifier;
        this.catalogClient = new CatalogClient(sparkConf);
    }

    // The doRefresh method should provide implementation on how to get the metadata location
    @Override
    public void doRefresh() {
        try {
            GetTableResponse table = catalogClient.getTable(GetTableRequest.newBuilder().setPath(getPath()).build());
            // When updating from a metadata file location, call the helper method
            refreshFromMetadataLocation(table.getTable().getMetadataLocation());
        } catch (CatalogClient.NotFoundException e) {
            refreshFromMetadataLocation(null);
        }

    }

    // The doCommit method should provide implementation on how to update with metadata location atomically
    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
        // Write new metadata using helper method
        String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
        if (base == null) {
            catalogClient.createTable(CreateTableRequest.newBuilder().build().newBuilder().setTable(
                    CatalogTable.newBuilder()
                            .setPath(getPath())
                            .setMetadataLocation(newMetadataLocation).build()
                    ).build()
            );
        } else {
            catalogClient.updateTable(UpdateTableRequest.newBuilder().setTable(
                    CatalogTable.newBuilder()
                            .setPath(getPath())
                            .setMetadataLocation(newMetadataLocation).build()
                    ).setPreviousMetadataLocation(base.metadataFileLocation()).build()
            );
        }
    }

    private String getPath() {
        return TableCatalogIdentifier.fromTableIdentifier(tableIdentifier).toPath();
    }

    // The io method provides a FileIO which is used to read and write the table metadata files
    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    public String metadataFileLocation(String filename) {
        return super.metadataFileLocation(filename);
    }

}
