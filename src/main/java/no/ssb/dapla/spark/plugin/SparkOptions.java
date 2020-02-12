package no.ssb.dapla.spark.plugin;

import scala.Option;
import scala.collection.immutable.Map;

public class SparkOptions {
    public static final String CURRENT_OPERATION = "spark.ssb.session.operation";
    public static final String CURRENT_NAMESPACE = "spark.ssb.session.namespace";
    static final String PATH = "PATH";
    static final String VALUATION = "valuation";
    static final String STATE = "state";

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

    private String getParameter(String option) {
        Option<String> optionalValue = parameters.get(option);
        if (optionalValue.isEmpty()) {
            throw new IllegalStateException(option + " missing from parameters" + parameters);
        }
        return optionalValue.get();
    }
}
