package no.ssb.dapla.spark.plugin.metadata;

import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.api.DatasetMeta;
import org.apache.avro.Schema;

public interface MetaDataWriter {
    void writeMetadataFile(String parentUri, DatasetMeta datasetMeta, ByteString validMetaJsonBytes);

    void writeSignatureFile(String parentUri, DatasetMeta datasetMeta, ByteString signatureBytes);

    void writeSchemaFile(String parentUri, DatasetMeta datasetMeta, Schema schema);
}
