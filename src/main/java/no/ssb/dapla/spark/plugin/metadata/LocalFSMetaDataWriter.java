package no.ssb.dapla.spark.plugin.metadata;

import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.utils.ProtobufJsonUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LocalFSMetaDataWriter implements MetaDataWriter {
    public static final String DATASET_META_FILE_NAME = "dataset-meta.json";

    public LocalFSMetaDataWriter() {
    }

    @Override
    public void write(DatasetMeta datasetMeta) throws IOException {
        final String storagePath = DatasetUri.of(datasetMeta.getParentUri(), datasetMeta.getId().getPath(),
                datasetMeta.getId().getVersion()).toString();
        String json = ProtobufJsonUtils.toString(datasetMeta);

        // Save as json with name: 'dataset-meta.json'
        Files.write(Paths.get(storagePath + "/" + DATASET_META_FILE_NAME), json.getBytes(), StandardOpenOption.CREATE);
    }
}
