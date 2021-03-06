package no.ssb.dapla.spark.plugin.metadata;

import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.api.DatasetMeta;

public class NoOpMetadataWriter implements MetaDataWriter {
    @Override
    public void writeMetadataFile(String parentUri, DatasetMeta datasetMeta, ByteString validMetaJsonBytes) {
        // Do nothing
    }

    @Override
    public void writeSignatureFile(String parentUri, DatasetMeta datasetMeta, ByteString signatureBytes) {
        // Do nothing
    }

    @Override
    public void writeSchemaFile(String parentUri, DatasetMeta datasetMeta, String schema) {
        // Do nothing
    }

    @Override
    public void writeLineageDocFile(String parentUri, DatasetMeta datasetMeta, String lineageDoc) {
        // Do nothing
    }
}
