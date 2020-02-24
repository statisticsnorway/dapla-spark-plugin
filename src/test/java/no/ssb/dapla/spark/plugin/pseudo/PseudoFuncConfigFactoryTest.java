package no.ssb.dapla.spark.plugin.pseudo;

import no.ssb.dapla.dlp.pseudo.func.PseudoFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFuncConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PseudoFuncConfigFactoryTest {

    @Test
    public void createFpeText_withRequiredArgs_shouldSucceed() {
        PseudoFuncConfig config = PseudoFuncConfigFactory.get("fpe-text( keyId123 )");
        assertThat(config.getRequired(PseudoFuncConfig.Param.FUNC_DECL, String.class)).isEqualTo("fpe-text(keyId123)");
        assertThat(config.has(PseudoFuncConfig.Param.FUNC_IMPL)).isTrue();
        assertThat(config.has(FpeFuncConfig.Param.ALPHABET)).isTrue();
        assertThat(config.getRequired(FpeFuncConfig.Param.KEY_ID, String.class)).isEqualTo("keyId123");
    }

    @Test
    public void createFpeText_withoutRequiredArgs_shouldThrowException() {
        assertThatThrownBy(
          () -> PseudoFuncConfigFactory.get("fpe-text()")
        ).isInstanceOf(PseudoFuncConfigFactory.PseudoFuncConfigException.class);
    }

    @Test
    public void createFpeText_withTooManyArgs_shouldThrowException() {
        assertThatThrownBy(
          () -> PseudoFuncConfigFactory.get("fpe-text( keyId123, foo )")
        ).isInstanceOf(PseudoFuncConfigFactory.PseudoFuncConfigException.class);
    }

    @Test
    public void create_undefinedFunction_shouldThrowException() {
        assertThatThrownBy(
          () -> PseudoFuncConfigFactory.get("blah")
        ).isInstanceOf(PseudoFuncConfigFactory.PseudoFuncNotDefinedException.class);
    }

}