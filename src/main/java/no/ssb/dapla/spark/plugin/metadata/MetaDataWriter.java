package no.ssb.dapla.spark.plugin.metadata;

import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.api.DatasetMeta;

public interface MetaDataWriter {
    void writeMetadataFile(DatasetMeta datasetMeta, ByteString validMetaJsonBytes);

    void writeSignatureFile(DatasetMeta datasetMeta, ByteString signatureBytes);
}
