package no.ssb.dapla.spark.plugin.metadata;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.Option;

public class MetaDataWriterFactory {

    static final String METADATA_WRITER = "spark.ssb.dapla.metadata.writer";
    private final SparkContext context;

    public MetaDataWriterFactory(SparkContext context) {
        this.context = context;
    }

    public static MetaDataWriterFactory fromSparkContext(SparkContext context) {
        return new MetaDataWriterFactory(context);
    }

    public MetaDataWriter create() {
        SparkConf conf = context.getConf();
        Option<String> option = conf.getOption(METADATA_WRITER);
        if (option.isEmpty()) {
            throw new IllegalArgumentException("Missing spark config: " + METADATA_WRITER);
        }

        if (option.get().equals(FilesystemMetaDataWriter.class.getName())) {
            return new FilesystemMetaDataWriter(context);
        } else if (option.get().equals(NoOpMetadataWriter.class.getName())) {
            return new NoOpMetadataWriter();
        }

        throw new IllegalStateException("No implementation found for " + option.get());
    }
}
