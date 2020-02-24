package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.fpe.AlphabetType;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFunc;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFuncConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class PseudoUDFsTest {

    private SQLContext sqlContext;

    @Before
    public void setUp() {
        SparkSession session = SparkSession.builder()
          .appName(PseudoUDFsTest.class.getSimpleName())
          .master("local")
          .getOrCreate();

        this.sqlContext = session.sqlContext();
    }

    @Test
    public void testUDFNameFormatting() {

        for (DataType dataType : PseudoUDFs.supportedDatatypes()) {
            assertThat(PseudoUDFs.udfNameOf("fpe-foo(key1)", PseudoUDFs.TransformationType.APPLY, dataType)).isEqualTo("pseudo_fpe-foo(key1)_apply_" + dataType.typeName().toLowerCase() + "_udf");
        }
    }

    @Test
    public void testRegisterUDFs() {
        Set<PseudoFuncConfig> configs = ImmutableSet.of(
            new PseudoFuncConfig(ImmutableMap.of(
              PseudoFuncConfig.Param.FUNC_DECL, "fpe-foo(keyId1)",
              PseudoFuncConfig.Param.FUNC_IMPL, FpeFunc.class.getName(),
              FpeFuncConfig.Param.ALPHABET, AlphabetType.ALPHANUMERIC_WHITESPACE,
              FpeFuncConfig.Param.KEY_ID, "keyId1",
              FpeFuncConfig.Param.KEY, ""
            )),

          new PseudoFuncConfig(ImmutableMap.of(
            PseudoFuncConfig.Param.FUNC_DECL, "fpe-bar(keyId2)",
            PseudoFuncConfig.Param.FUNC_IMPL, FpeFunc.class.getName(),
            FpeFuncConfig.Param.ALPHABET, AlphabetType.DIGITS,
            FpeFuncConfig.Param.KEY_ID, "keyId2",
            FpeFuncConfig.Param.KEY, ""
          ))
        );

        assertThat(PseudoUDFs.registerUDFs(sqlContext, configs)).containsExactly(
          "pseudo_fpe-bar(keyid2)_apply_double_udf",
          "pseudo_fpe-bar(keyid2)_apply_timestamp_udf",
          "pseudo_fpe-foo(keyid1)_restore_string_udf",
          "pseudo_fpe-bar(keyid2)_apply_integer_udf",
          "pseudo_fpe-bar(keyid2)_apply_long_udf",
          "pseudo_fpe-bar(keyid2)_restore_double_udf",
          "pseudo_fpe-foo(keyid1)_apply_boolean_udf",
          "pseudo_fpe-bar(keyid2)_restore_integer_udf",
          "pseudo_fpe-foo(keyid1)_restore_float_udf",
          "pseudo_fpe-foo(keyid1)_apply_date_udf",
          "pseudo_fpe-bar(keyid2)_restore_float_udf",
          "pseudo_fpe-foo(keyid1)_apply_string_udf",
          "pseudo_fpe-foo(keyid1)_restore_double_udf",
          "pseudo_fpe-foo(keyid1)_restore_long_udf",
          "pseudo_fpe-bar(keyid2)_restore_boolean_udf",
          "pseudo_fpe-bar(keyid2)_apply_date_udf",
          "pseudo_fpe-foo(keyid1)_apply_float_udf",
          "pseudo_fpe-foo(keyid1)_restore_integer_udf",
          "pseudo_fpe-bar(keyid2)_restore_string_udf",
          "pseudo_fpe-foo(keyid1)_restore_date_udf",
          "pseudo_fpe-foo(keyid1)_apply_long_udf",
          "pseudo_fpe-bar(keyid2)_restore_date_udf",
          "pseudo_fpe-foo(keyid1)_restore_timestamp_udf",
          "pseudo_fpe-foo(keyid1)_apply_double_udf",
          "pseudo_fpe-foo(keyid1)_restore_boolean_udf",
          "pseudo_fpe-foo(keyid1)_apply_timestamp_udf",
          "pseudo_fpe-bar(keyid2)_restore_timestamp_udf",
          "pseudo_fpe-bar(keyid2)_apply_string_udf",
          "pseudo_fpe-foo(keyid1)_apply_integer_udf",
          "pseudo_fpe-bar(keyid2)_restore_long_udf",
          "pseudo_fpe-bar(keyid2)_apply_float_udf",
          "pseudo_fpe-bar(keyid2)_apply_boolean_udf"
        );
    }
}