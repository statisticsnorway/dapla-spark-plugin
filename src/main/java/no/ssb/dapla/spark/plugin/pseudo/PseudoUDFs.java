package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.collect.ImmutableSet;
import no.ssb.dapla.dlp.pseudo.func.PseudoFunc;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncFactory;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncInput;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncOutput;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.APPLY;
import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.RESTORE;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

public final class PseudoUDFs {
    private static final Set<DataType> SUPPORTED_DATATYPES = ImmutableSet.of(
      StringType, LongType, IntegerType, DoubleType, FloatType, BooleanType, DateType, TimestampType
    );

    private PseudoUDFs() {
    }

    static Set<DataType> supportedDatatypes() {
        return SUPPORTED_DATATYPES;
    }

    public static boolean isSupportedDataType(DataType dataType) {
        return SUPPORTED_DATATYPES.contains(dataType);
    }

    /** DataType specific UDF name */
    public static String udfNameOf(String funcDecl, TransformationType transType, DataType dataType) {
        return String.format("pseudo_%s_%s_%s_udf", funcDecl, transType, dataType.typeName()).toLowerCase();
    }

    public static Set<String> registerUDFs(SQLContext sqlContext, Iterable<PseudoFuncConfig> configs) {
        Set<String> registeredNames = new HashSet<>();

        configs.forEach(funcConfig ->
            SUPPORTED_DATATYPES.forEach(dataType -> {
                String applyName = udfNameOf(funcConfig.getFuncDecl(), APPLY, dataType);
                sqlContext.udf().register(applyName, new ApplyPseudoUDF(funcConfig), dataType);
                registeredNames.add(applyName);

                String restoreName = udfNameOf(funcConfig.getFuncDecl(), RESTORE, dataType);
                sqlContext.udf().register(restoreName, new RestorePseudoUDF(funcConfig), dataType);
                registeredNames.add(restoreName);
            })
        );

        return registeredNames;
    }

    enum TransformationType {
        APPLY, RESTORE
    }

    abstract static class AbstractPseudoUDF implements UDF1<Object, Object> {
        protected final PseudoFuncConfig pseudoFuncConfig;

        public AbstractPseudoUDF(PseudoFuncConfig pseudoFuncConfig) {
            this.pseudoFuncConfig = pseudoFuncConfig;
        }
    }

    static class ApplyPseudoUDF extends AbstractPseudoUDF {
        public ApplyPseudoUDF(PseudoFuncConfig pseudoFuncConfig) {
            super(pseudoFuncConfig);
        }

        @Override
        public Object call(Object data) throws Exception {
            PseudoFunc func = PseudoFuncFactory.create(pseudoFuncConfig);
            PseudoFuncOutput output = func.apply(PseudoFuncInput.of(data));
            return output.getFirstValue();
        }
    }

    static class RestorePseudoUDF extends AbstractPseudoUDF {
        public RestorePseudoUDF(PseudoFuncConfig pseudoFuncConfig) {
            super(pseudoFuncConfig);
        }

        @Override
        public Object call(Object data) throws Exception {
            PseudoFunc func = PseudoFuncFactory.create(pseudoFuncConfig);
            PseudoFuncOutput output = func.restore(PseudoFuncInput.of(data));
            return output.getFirstValue();
        }
    }

}
