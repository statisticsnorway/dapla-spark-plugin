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
 *     .option("pseudo", "INCOME=fpe-digits(keyId1),MUNICIPALITY=fpe-digits(keyId2),DATA_QUALITY=fpe-alphanumeric(keyId3)")
 *     .option("valuation", "SENSITIVE")
 *     .option("state", "RAW")
 *     .mode("overwrite")
 *     .save(outfilePath)
 * </pre>
 *
 * The PseudoOptions can be parsed using the PseudoOptions.Parser and will contain a
 * var -> pseudo function id mapping.
 */
public class PseudoOptions {

    private static final String PSEUDO_PARAM = "pseudo";
    private static final String UNDEFINED = "undefined";
    private final Map<String, String> varToFunctionMap = new LinkedHashMap();

    void addPseudoFunctionMapping(String var, String functionDecl) {
        if (functionDecl != null) {
            varToFunctionMap.put(var, functionDecl);
        }
    }

    public Set<String> vars() {
        return ImmutableSet.copyOf(varToFunctionMap.keySet());
    }

    public Set<String> functions() {
        return ImmutableSet.copyOf(varToFunctionMap.values());
    }

    public Optional<String> pseudoFunc(String var) {
        return Optional.ofNullable(varToFunctionMap.get(var));
    }

    /** Create from JSON */
    public static PseudoOptions fromJson(String json) {
        PseudoOptions options = new PseudoOptions();
        try {
            Json.toObject(new TypeReference<List<ConfigElement>>() {}, json).forEach(
              e -> options.addPseudoFunctionMapping(e.var, e.pseudoFunc));
        }
        catch (Exception e) { /* swallow */ }

        return options;
    }

    /** Convert to JSON */
    public String toJson() {
        return Json.from(
          varToFunctionMap.keySet().stream()
          .map(var -> new ConfigElement(var, varToFunctionMap.get(var)))
          .collect(Collectors.toList())
        );
    }

    @Override
    public String toString() {
        return varToFunctionMap.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PseudoOptions options = (PseudoOptions) o;
        return Objects.equals(varToFunctionMap, options.varToFunctionMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(varToFunctionMap);
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

    /** Used for json serialization */
    private static class ConfigElement {
        private String var;
        private String pseudoFunc;

        private ConfigElement() {}

        public ConfigElement(String var, String pseudoFunc) {
            this.var = var;
            this.pseudoFunc = pseudoFunc;
        }

        public String getVar() {
            return var;
        }

        public String getPseudoFunc() {
            return pseudoFunc;
        }
    }

}
