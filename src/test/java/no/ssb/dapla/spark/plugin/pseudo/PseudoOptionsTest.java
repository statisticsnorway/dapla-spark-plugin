package no.ssb.dapla.spark.plugin.pseudo;

import org.junit.Test;
import scala.collection.immutable.Map;

import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PseudoOptionsTest {

    @Test
    public void commaSeparatedPseudoOptions_parse_shouldParseLeniently() {
        Map<String, String> params = new Map.Map1("pseudo", "COLUMN1=foo,COLUMN2=bar");
        PseudoOptions options = PseudoOptions.parse(params).orElseThrow(() -> new RuntimeException());
        assertThat(options.columns().size()).isEqualTo(2);
        assertThat(options.columnFunc("COLUMN1")).isEqualTo(Optional.of("foo"));
        assertThat(options.columnFunc("COLUMN2")).isEqualTo(Optional.of("bar"));

        params = new Map.Map1("pseudo", "COLUMN1=, COLUMN2 = bar");
        options = PseudoOptions.parse(params).orElseThrow(() -> new RuntimeException());
        assertThat(options.columnFunc("COLUMN1")).isEqualTo(Optional.empty());
        assertThat(options.columnFunc("COLUMN2")).isEqualTo(Optional.of("bar"));
        assertThat(options.columns().size()).isEqualTo(1);
        assertThat(options.columnFunc("NONEXISTENTCOLUMN")).isEqualTo(Optional.empty());

        params = new Map.Map1("pseudo", "=foo,  ,,,,,COLUMN2=");
        options = PseudoOptions.parse(params).orElseThrow(() -> new RuntimeException());
        assertThat(options.columns().size()).isEqualTo(0);

        params = new Map.Map1("foo", "bar"); // no pseudo-option present
        assertThat(PseudoOptions.parse(params)).isEqualTo(Optional.empty());
    }

}