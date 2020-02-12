package no.ssb.dapla.spark.plugin.pseudo;

import org.junit.Test;
import scala.collection.immutable.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class PseudoOptionsTest {
    @Test
    public void writeOptions_validInput_shouldParseSuccessfully() {
        PseudoOptions opts = PseudoOptions.parse(new Map.Map1<>(
          "pseudo", "col1=func1(key111, key222);col2=func2 ();; ; col3=func1"
        )).orElse(null);
        assertThat(opts.vars().size()).isEqualTo(3);
        assertThat(opts.pseudoFunc("col1").orElse(null)).isEqualTo("func1(key111,key222)");
        assertThat(opts.pseudoFunc("col2").orElse(null)).isEqualTo("func2()");
        assertThat(opts.pseudoFunc("col3").orElse(null)).isEqualTo("func1()");
    }

    @Test
    public void writeOptions_emptyInput_shouldBeDefinedAsEmptySet() {
        PseudoOptions opts = PseudoOptions.parse(new Map.Map1<>(
          "pseudo", ""
        )).orElse(null);
        assertThat(opts.vars().size()).isEqualTo(0);

        opts = PseudoOptions.parse(new Map.Map1<>(
          "nothing", ""
        )).orElse(null);
        assertThat(opts).isNull();

    }

    @Test
    public void writeOptions_whenPseudoIsNotDefined_shouldNotBeDefined() {
        PseudoOptions opts = PseudoOptions.parse(new Map.Map1<>(
          "nothing", ""
        )).orElse(null);
        assertThat(opts).isNull();
    }

    @Test
    public void readOptions_validInput_shouldParseSuccessfully() {
        PseudoOptions opts = PseudoOptions.parse(new Map.Map1<>(
          "pseudo", "col1;col2;; ; col3=func3"
        )).orElse(null);
        assertThat(opts.vars().size()).isEqualTo(3);
        assertThat(opts.pseudoFunc("col1").orElse(null)).isEqualTo("undefined");
        assertThat(opts.pseudoFunc("col2").orElse(null)).isEqualTo("undefined");
        assertThat(opts.pseudoFunc("col3").orElse(null)).isEqualTo("func3()");
    }

    @Test
    public void toJson_whenNoPseudoFuncsAreDefined_shouldReturnEmptyArray() {
        PseudoOptions opts = PseudoOptions.parse(new Map.Map1<>(
          "pseudo", ""
        )).orElse(null);
        assertThat(opts.toJson()).isEqualTo("[]");
    }

    @Test
    public void toJson_withPseudoFuncs_shouldReturnJson() {
        PseudoOptions opts = PseudoOptions.parse(new Map.Map1<>(
          "pseudo", " ;  col1=func1;col2=func2(someParam);; ; col3;;        ;    "
        )).orElse(null);
        assertThat(opts.toJson()).isEqualTo("[{\"col\":\"col1\",\"pseudoFunc\":\"func1()\"},{\"col\":\"col2\",\"pseudoFunc\":\"func2(someParam)\"},{\"col\":\"col3\",\"pseudoFunc\":\"undefined\"}]");
    }

    @Test
    public void fromJson_invalidJson_shouldReturnEmptyOptions() {
        assertThat(PseudoOptions.fromJson(null).vars().size()).isEqualTo(0);
        assertThat(PseudoOptions.fromJson("gibberish").vars().size()).isEqualTo(0);
    }

    @Test
    public void fromJson_jsonWithPseudoFuncs_shouldParseSuccessfully() {
        String json = "[{\"col\":\"col1\",\"pseudoFunc\":\"func1()\"},{\"col\":\"col2\",\"pseudoFunc\":\"func2(someParam)\"},{\"col\":\"col3\",\"pseudoFunc\":\"undefined\"}]";
        assertThat(PseudoOptions.fromJson(json).vars()).contains("col1", "col2", "col3");
    }

}