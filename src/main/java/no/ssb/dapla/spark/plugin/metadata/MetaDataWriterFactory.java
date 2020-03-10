package no.ssb.dapla.spark.plugin.metadata;

import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import scala.Option;

public class MetaDataWriterFactory {

    static final String METADATA_WRITER = "spark.ssb.dapla.metadata.writer";
    private final SparkSession sparkSession;

    public MetaDataWriterFactory(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static MetaDataWriterFactory fromSparkSession(SparkSession context) {
        return new MetaDataWriterFactory(context);
    }

    public MetaDataWriter create() {
        RuntimeConfig conf = sparkSession.conf();
        Option<String> option = conf.getOption(METADATA_WRITER);
        if (option.isEmpty()) {
            throw new IllegalArgumentException("Missing spark config: " + METADATA_WRITER);
        }

        if (option.get().equals(FilesystemMetaDataWriter.class.getName())) {
            return new FilesystemMetaDataWriter(sparkSession);
        } else if (option.get().equals(NoOpMetadataWriter.class.getName())) {
            return new NoOpMetadataWriter();
        }

        throw new IllegalStateException("No implementation found for " + option.get());
    }
}
