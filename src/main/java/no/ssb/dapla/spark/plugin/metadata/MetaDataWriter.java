package no.ssb.dapla.spark.plugin.metadata;

import no.ssb.dapla.dataset.api.DatasetMeta;

import java.io.IOException;

public interface MetaDataWriter {
    void write(DatasetMeta datasetMeta) throws IOException;
}
