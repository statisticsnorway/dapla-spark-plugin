package no.ssb.dapla.spark.plugin.pseudo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import no.ssb.dapla.dlp.pseudo.func.util.Json;
import no.ssb.dapla.spark.plugin.pseudo.PseudoFuncConfigFactory.FuncDeclaration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
    private static final String UNDEFINED = "undefined";
    private final Map<String, String> columnToFunctionMap = new LinkedHashMap();

    void addPseudoFunctionMapping(String column, String functionDecl) {
        if (functionDecl != null) {
            columnToFunctionMap.put(column, functionDecl);
        }
    }

    public Set<String> columns() {
        return ImmutableSet.copyOf(columnToFunctionMap.keySet());
    }

    public Set<String> functions() {
        return ImmutableSet.copyOf(columnToFunctionMap.values());
    }

    public Optional<String> columnFunc(String column) {
        return Optional.ofNullable(columnToFunctionMap.get(column));
    }

    /** Create from JSON */
    public static PseudoOptions fromJson(String json) {
        PseudoOptions options = new PseudoOptions();
        try {
            Json.toObject(new TypeReference<List<ConfigElement>>() {}, json).forEach(
              e -> options.addPseudoFunctionMapping(e.col, e.pseudoFunc));
        }
        catch (Exception e) { /* swallow */ }

        return options;
    }

    /** Convert to JSON */
    public String toJson() {
        return Json.from(
          columnToFunctionMap.keySet().stream()
          .map(col -> new ConfigElement(col, columnToFunctionMap.get(col)))
          .collect(Collectors.toList())
        );
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

    private static class Parser {
        private Parser() {}
        private static final Splitter COMMA_SPLITTER = Splitter.on(";").omitEmptyStrings().trimResults();
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
                    FuncDeclaration funcDecl = FuncDeclaration.fromString(keyAndValue.get(1));
                    options.addPseudoFunctionMapping(keyAndValue.get(0), funcDecl.toString());
                }
                else if (keyAndValue.size() == 1) {
                    options.addPseudoFunctionMapping(keyAndValue.get(0), UNDEFINED);
                }

            }

            return Optional.of(options);
        }
    }

    /** Used for json serde */
    private static class ConfigElement {
        private String col;
        private String pseudoFunc;

        private ConfigElement() {}

        public ConfigElement(String col, String pseudoFunc) {
            this.col = col;
            this.pseudoFunc = pseudoFunc;
        }

        public String getCol() {
            return col;
        }

        public String getPseudoFunc() {
            return pseudoFunc;
        }
    }

}
