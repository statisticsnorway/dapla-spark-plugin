package no.ssb.dapla.spark.plugin.metadata;

import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.spark.plugin.DaplaSparkConfig;
import no.ssb.dapla.utils.ProtobufJsonUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LocalFSMetaDataWriter implements MetaDataWriter {
    public static final String DATASET_META_FILE_NAME = "dataset-meta.json";

    private final DaplaSparkConfig daplaSparkConfig;

    public LocalFSMetaDataWriter(DaplaSparkConfig daplaSparkConfig) {
        this.daplaSparkConfig = daplaSparkConfig;
    }

    @Override
    public void write(DatasetMeta datasetMeta) throws IOException {
        String storagePath = daplaSparkConfig.getStoragePath();
        String json = ProtobufJsonUtils.toString(datasetMeta);

        // Save as json with name: 'dataset-meta.json'
        Files.write(Paths.get(storagePath + "/" + DATASET_META_FILE_NAME), json.getBytes());
    }
}
