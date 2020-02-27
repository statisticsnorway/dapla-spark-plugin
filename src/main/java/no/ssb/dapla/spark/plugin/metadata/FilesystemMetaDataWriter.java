package no.ssb.dapla.spark.plugin.metadata;

import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;

import java.io.IOException;
import java.net.URI;

public class FilesystemMetaDataWriter implements MetaDataWriter {

    public static final String DATASET_META_FILE_NAME = "dataset-meta.json";
    private final SparkContext context;

    public FilesystemMetaDataWriter(SparkContext context) {
        this.context = context;
    }

    @Override
    public void write(DatasetMeta datasetMeta) {
        final URI storagePath = DatasetUri.of(datasetMeta.getParentUri(), datasetMeta.getId().getPath(),
                datasetMeta.getId().getVersion()).toURI();
        try {
            String body = ProtobufJsonUtils.toString(datasetMeta);
            FileSystem fs = FileSystem.get(storagePath, context.hadoopConfiguration());
            FSDataOutputStream outputStream = fs.create(new Path(storagePath  + "/" + DATASET_META_FILE_NAME));
            IOUtils.write(body, outputStream);
        } catch (IOException e) {
            throw new RuntimeException("Error writing metadata file", e);
        }
    }
}
