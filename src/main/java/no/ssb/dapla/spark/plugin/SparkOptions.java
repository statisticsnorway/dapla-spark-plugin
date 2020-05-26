package no.ssb.dapla.spark.plugin;

import scala.Option;
import scala.collection.immutable.Map;

public class SparkOptions {
    public static final String ACCESS_TOKEN = "spark.ssb.session.token";
    public static final String ACCESS_TOKEN_EXP = "spark.ssb.session.token.exp";

    static final String PATH = "PATH";
    static final String VALUATION = "valuation";
    static final String STATE = "state";
    static final String VERSION = "version";
    static final String METADATA = "metadata";

    final Map<String, String> parameters;

    public SparkOptions(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    String getPath() {
        return getParameter(PATH);
    }

    String getValuation() {
        return getParameter(VALUATION);
    }

    String getState() {
        return getParameter(STATE);
    }

    String getVersion() {
        return getParameter(VERSION);
    }

    String getMetadata() {
        return getParameter(METADATA);
    }

    private String getParameter(String option) {
        Option<String> optionalValue = parameters.get(option);
        if (optionalValue.isEmpty()) {
            return null;
        }
        return optionalValue.get();
    }
}
