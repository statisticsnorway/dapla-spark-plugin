package no.ssb.dapla.spark.plugin.metadata;

import org.apache.spark.SparkConf;
import scala.Option;

public class MetaDataWriterFactory {

    static final String METADATA_WRITER = "spark.ssb.dapla.metadata.writer";
    private final SparkConf conf;

    private MetaDataWriterFactory(SparkConf conf) {
        this.conf = conf;
    }

    public static MetaDataWriterFactory fromSparkConf(SparkConf conf) {
        return new MetaDataWriterFactory(conf);
    }

    public MetaDataWriter create() {
        Option<String> option = conf.getOption(METADATA_WRITER);
        if (option.isEmpty()) {
            throw new IllegalArgumentException("Missing spark config: " + METADATA_WRITER);
        }

        if (option.get().equals(LocalFSMetaDataWriter.class.getName())) {
            return new LocalFSMetaDataWriter();
        } else if (option.get().equals(GoogleCSMetaDataWriter.class.getName())) {
            return new GoogleCSMetaDataWriter();
        } else if (option.get().equals(NoOpMetadataWriter.class.getName())) {
            return new NoOpMetadataWriter();
        }

        throw new IllegalStateException("No implementation found for " + option.get());
    }
}
