package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.base.Splitter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Wraps pseudonymization options and provides convenience functionality to parse and access these.

 * E.g. given:
 * <pre>
 * var df = data.write
 *     .format("no.ssb.dapla.spark.plugin")
 *     .option("pseudo", "INCOME=fpe-digits-123,MUNICIPALITY=fpe-digits-123,DATA_QUALITY=fpe-alphanumeric-123")
 *     .option("valuation", "SENSITIVE")
 *     .option("state", "RAW")
 *     .mode("overwrite")
 *     .save(outfilePath)
 * </pre>
 *
 * The PseudoOptions can be parsed using the PseudoOptions.Parser and will contain a
 * columnName -> pseudo function id mapping.
 */
public class PseudoOptions {

    private static final String PSEUDO_PARAM = "pseudo";
    private final Map<String, String> columnToFunctionMap = new HashMap();

    void addPseudoFunctionMapping(String column, String functionName) {
        if (functionName != null) {
            columnToFunctionMap.put(column, functionName);
        }
    }

    public Set<String> columns() {
        return columnToFunctionMap.keySet();
    }

    public Optional<String> columnFunc(String column) {
        return Optional.ofNullable(columnToFunctionMap.get(column));
    }

    @Override
    public String toString() {
        return columnToFunctionMap.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PseudoOptions options = (PseudoOptions) o;
        return Objects.equals(columnToFunctionMap, options.columnToFunctionMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnToFunctionMap);
    }

    public static Optional<PseudoOptions> parse(scala.collection.immutable.Map<String, String> params) {
        return Parser.parse(params);
    }

    static class Parser {
        private static final Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
        private static final Splitter EQUALS_SPLITTER = Splitter.on("=").omitEmptyStrings().trimResults();

        public static Optional<PseudoOptions> parse(scala.collection.immutable.Map<String, String> params) {
            if (! params.contains(PSEUDO_PARAM)) {
                return Optional.empty();
            }

            String commaSeparatedPseudoOptions = params.get(PSEUDO_PARAM).get();
            PseudoOptions options = new PseudoOptions();
            for (String mapping : COMMA_SPLITTER.split(commaSeparatedPseudoOptions)) {
                List<String> keyAndValue = EQUALS_SPLITTER.splitToList(mapping);
                if (keyAndValue.size() == 2) {
                    options.addPseudoFunctionMapping(keyAndValue.get(0), keyAndValue.get(1));
                }
            }

            return Optional.of(options);
        }
    }
}
