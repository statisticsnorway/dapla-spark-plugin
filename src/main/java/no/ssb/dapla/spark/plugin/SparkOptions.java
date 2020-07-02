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
    static final String SCHEMA_DOC = "dataset-doc";
    static final String LINEAGE_DOC = "lineage-doc";
    static final String AUTO_REPARTITION = "auto-repartition";

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

    String getDoc() {
        return getParameter(SCHEMA_DOC);
    }
    String getLineageDoc() {
        return getParameter(LINEAGE_DOC);
    }

    boolean getAutoRepartition() {
        return Boolean.valueOf(parameters.getOrElse(AUTO_REPARTITION, () -> "true"));
    }

    private String getParameter(String option) {
        Option<String> optionalValue = parameters.get(option);
        if (optionalValue.isEmpty()) {
            return null;
        }
        return optionalValue.get();
    }
}
