package no.ssb.dapla.spark.plugin.metadata;

import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.api.DatasetMeta;

public class NoOpMetadataWriter implements MetaDataWriter {
    @Override
    public void writeMetadataFile(DatasetMeta datasetMeta, ByteString validMetaJsonBytes) {
        // Do nothing
    }

    @Override
    public void writeSignatureFile(DatasetMeta datasetMeta, ByteString signatureBytes) {
        // Do nothing
    }
}
