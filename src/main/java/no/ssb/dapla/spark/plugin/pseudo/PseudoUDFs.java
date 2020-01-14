package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncInput;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncOutput;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncRegistry;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataType;

import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public final class PseudoUDFs {
    public enum Transformation {
        APPLY, RESTORE
    }

    private static final PseudoFuncRegistry REGISTRY = PseudoFuncRegistryFactory.newInstance();

    private static final Set<DataType> SUPPORTED_DATATYPES = ImmutableSet.of(
      StringType, LongType, IntegerType, DoubleType, FloatType, BooleanType, DateType, TimestampType
    );

    public static final Map<Transformation, UDF2<String,Object,Object>> BY_TRANSFORMATION = ImmutableMap.of(
      Transformation.APPLY, (UDF2<String, Object, Object>) (funcName, data) -> {
          PseudoFuncOutput output = REGISTRY.get(funcName).apply(PseudoFuncInput.of(data));
          return output.getValues().get(0); // TODO: Fix this
      },

      Transformation.RESTORE, (UDF2<String, Object, Object>) (funcName, data) -> {
          PseudoFuncOutput output = REGISTRY.get(funcName).restore(PseudoFuncInput.of(data));
          return output.getValues().get(0); // TODO: Fix this
      }
    );

    private PseudoUDFs() {
    }

    static Set<DataType> supportedDatatypes() {
        return SUPPORTED_DATATYPES;
    }

    static boolean isSupportedDataType(DataType dataType) {
        return SUPPORTED_DATATYPES.contains(dataType);
    }
}
