package no.ssb.dapla.spark.plugin;

import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class PathValidatorTest {

    @Test
    public void testValidReadPaths() {
        PathValidator.validateRead("/skatt/person/no_space/tabell-2020/");
        PathValidator.validateRead("/skatt/person/*");
        PathValidator.validateRead("/skatt*");
    }

    @Test
    public void testInvalidReadPaths() {
        assertThatThrownBy(() -> PathValidator.validateRead("")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateRead("/")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateRead("skatt/person")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateRead("skatt/person*")).isInstanceOf(PathValidator.ValidationException.class);
        // should only accept * at the end of string
        assertThatThrownBy(() -> PathValidator.validateRead("/skatt/person/*/no")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateRead("/skatt/person/with space")).isInstanceOf(PathValidator.ValidationException.class);
    }

    @Test
    public void testValidWritePaths() {
        PathValidator.validateWrite("/skatt/person/no_space/tabell-2020/");
        PathValidator.validateWrite("/skatt/person/");
    }

    @Test
    public void testInvalidWritePaths() {
        assertThatThrownBy(() -> PathValidator.validateWrite("")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateWrite("/")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateWrite("skatt/person")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateWrite("/skatt/person*")).isInstanceOf(PathValidator.ValidationException.class);
        assertThatThrownBy(() -> PathValidator.validateWrite("/skatt/person/with space")).isInstanceOf(PathValidator.ValidationException.class);
    }
}
