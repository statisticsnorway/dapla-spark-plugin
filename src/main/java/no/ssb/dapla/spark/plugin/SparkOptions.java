package no.ssb.dapla.spark.plugin;

import scala.Option;
import scala.collection.immutable.Map;

public class SparkOptions {
    public static final String ACCESS_TOKEN = "spark.ssb.session.token";
    public static final String ACCESS_TOKEN_EXP = "spark.ssb.session.token.exp";

    //public static final String CURRENT_OPERATION = "spark.ssb.session.operation";
    //public static final String CURRENT_NAMESPACE = "spark.ssb.session.namespace";
    //public static final String CURRENT_USER = "spark.ssb.session.userId";
    //public static final String CURRENT_DATASET_VERSION = "spark.ssb.session.dataset_version";
    //public static final String CURRENT_DATASET_META_JSON = "spark.ssb.session.dataset.json";
    //public static final String CURRENT_DATASET_META_JSON_SIGNATURE = "spark.ssb.session.dataset.signature";
    static final String PATH = "PATH";
    static final String VALUATION = "valuation";
    static final String STATE = "state";
    static final String VERSION = "version";

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

    private String getParameter(String option) {
        Option<String> optionalValue = parameters.get(option);
        if (optionalValue.isEmpty()) {
            return null;
        }
        return optionalValue.get();
    }
}
