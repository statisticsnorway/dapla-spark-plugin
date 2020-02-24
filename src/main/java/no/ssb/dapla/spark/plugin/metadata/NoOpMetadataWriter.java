package no.ssb.dapla.spark.plugin.metadata;

import no.ssb.dapla.dataset.api.DatasetMeta;

public class NoOpMetadataWriter implements MetaDataWriter {
    @Override
    public void write(DatasetMeta datasetMeta) {
        // Do nothing
    }
}
