package no.ssb.dapla.spark.plugin.metadata;

import com.google.protobuf.ByteString;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

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
        Set<String> addedProperties = new LinkedHashSet<>();
        Map<String, String> oldProperties = new LinkedHashMap<>();
        JavaConversions.mapAsJavaMap(sparkSession.conf().getAll()).entrySet().stream()
                .filter(e -> e.getKey().startsWith("spark."))
                .forEach(e -> {
                    String oldValue = hadoopConfiguration.get(e.getKey());
                    if (oldValue == null) {
                        addedProperties.add(e.getKey());
                    } else {
                        oldProperties.put(e.getKey(), oldValue);
                    }
                    hadoopConfiguration.set(e.getKey(), e.getValue());
                });
        try (
                FileSystem fs = FileSystem.get(storagePath, hadoopConfiguration);
                FSDataOutputStream outputStream = fs.create(metadataPath, false);
        ) {
            System.out.println("Write file path: " + metadataPath);
            IOUtils.write(content.toByteArray(), outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error writing metadata file", e);
        } finally {
            for (String addedProperty : addedProperties) {
                hadoopConfiguration.unset(addedProperty);
            }
            for (Map.Entry<String, String> e : oldProperties.entrySet()) {
                hadoopConfiguration.set(e.getKey(), e.getValue());
            }
        }
    }
}
