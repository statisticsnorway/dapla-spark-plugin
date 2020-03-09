package no.ssb.dapla.spark.plugin.metadata;

import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.spark.plugin.SparkOptions;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;

public class FilesystemMetaDataWriter implements MetaDataWriter {

    public static final String DATASET_META_FILE_NAME = ".dataset-meta.json";
    public static final String DATASET_META_SIGNATURE_FILE_NAME = ".dataset-meta.json.sign";
    private final SparkSession sparkSession;

    public FilesystemMetaDataWriter(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    @Override
    public void writeMetadataFile(DatasetMeta datasetMeta, ByteString validMetaJsonBytes) {
        writeFile(datasetMeta, validMetaJsonBytes, DATASET_META_FILE_NAME);
    }

    @Override
    public void writeSignatureFile(DatasetMeta datasetMeta, ByteString signatureBytes) {
        writeFile(datasetMeta, signatureBytes, DATASET_META_SIGNATURE_FILE_NAME);
    }

    private void writeFile(DatasetMeta datasetMeta, ByteString content, String filename) {
        final URI storagePath = DatasetUri.of(datasetMeta.getParentUri(), datasetMeta.getId().getPath(),
                datasetMeta.getId().getVersion()).toURI();
        final Path metadataPath = new Path(storagePath + Path.SEPARATOR + filename);
        Configuration hadoopConfiguration = sparkSession.sparkContext().hadoopConfiguration();
        hadoopConfiguration.set(SparkOptions.CURRENT_OPERATION, sparkSession.conf().get(SparkOptions.CURRENT_OPERATION));
        hadoopConfiguration.set(SparkOptions.CURRENT_NAMESPACE, sparkSession.conf().get(SparkOptions.CURRENT_NAMESPACE));
        hadoopConfiguration.set(SparkOptions.CURRENT_USER, sparkSession.conf().get(SparkOptions.CURRENT_USER));
        try (
                FileSystem fs = FileSystem.get(storagePath, hadoopConfiguration);
                FSDataOutputStream outputStream = fs.create(metadataPath, false);
        ) {
            System.out.println("Write file path: " + metadataPath);
            IOUtils.write(content.toByteArray(), outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error writing metadata file", e);
        } finally {
            hadoopConfiguration.unset(SparkOptions.CURRENT_OPERATION);
            hadoopConfiguration.unset(SparkOptions.CURRENT_NAMESPACE);
            hadoopConfiguration.unset(SparkOptions.CURRENT_USER);
        }
    }
}
