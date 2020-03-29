package no.ssb.dapla.spark.plugin;

import io.jaegertracing.Configuration;
import io.opentracing.Tracer;
import org.apache.spark.SparkConf;

import java.util.Optional;

import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;

public class TracerFactory {

    public static Optional<Tracer> createTracer(final SparkConf sparkConf) {
        if (!sparkConf.contains(CONFIG_ROUTER_OAUTH_TRACING_URL)) {
            return Optional.empty();
        }
        Configuration config = Configuration.fromEnv("dapla-spark-plugin")
                .withSampler(Configuration.SamplerConfiguration.fromEnv()
                        .withType("const")
                        .withParam(Integer.valueOf(1)))
                .withReporter(Configuration.ReporterConfiguration.fromEnv()
                        .withSender(Configuration.SenderConfiguration.fromEnv()
                                .withEndpoint(sparkConf.get(CONFIG_ROUTER_OAUTH_TRACING_URL))
                                .withAuthToken(sparkConf.get(SPARK_SSB_ACCESS_TOKEN)))
                );
        return Optional.of(config.getTracer());
    }

}
