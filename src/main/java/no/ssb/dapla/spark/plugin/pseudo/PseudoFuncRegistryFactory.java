package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncRegistry;
import no.ssb.dapla.dlp.pseudo.func.fpe.AlphabetType;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFunc;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFuncConfig;

import java.util.List;

public class PseudoFuncRegistryFactory {
    private static final String KEY_ID = "01DWENC90WW9K41EN0QS2Q23X4"; // Used in DaplaKeyServiceDummyImpl

    // This config should be fetched from elsewhere instead of being hardcoded
    static final List<PseudoFuncConfig> PSEUDO_CONFIG = ImmutableList.of(
      new PseudoFuncConfig(ImmutableMap.of(
        PseudoFuncConfig.Param.FUNC_NAME, "fpe-digits",
        PseudoFuncConfig.Param.FUNC_IMPL, FpeFunc.class.getName(),
        FpeFuncConfig.Param.ALPHABET, AlphabetType.DIGITS,
        FpeFuncConfig.Param.KEY_ID, KEY_ID
      )),
      new PseudoFuncConfig(ImmutableMap.of(
        PseudoFuncConfig.Param.FUNC_NAME, "fpe-alphanumeric",
        PseudoFuncConfig.Param.FUNC_IMPL, FpeFunc.class.getName(),
        FpeFuncConfig.Param.ALPHABET, AlphabetType.ALPHANUMERIC,
        FpeFuncConfig.Param.KEY_ID, KEY_ID
      ))
    );

    public static PseudoFuncRegistry newInstance() {
        PseudoFuncRegistry registry = new PseudoFuncRegistry();
        registry.init(PSEUDO_CONFIG);
        return registry;
    }
}
