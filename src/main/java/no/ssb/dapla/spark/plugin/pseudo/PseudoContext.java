package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import scala.collection.immutable.Map;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class PseudoContext {

    private final Set<String> registeredUDFs = new HashSet<>();
    private final Optional<PseudoOptions> pseudoOptions;

    public PseudoContext(SQLContext sqlContext, Map<String, String> parameters) {
        this.pseudoOptions = PseudoOptions.parse(parameters);
        PseudoUDFs.BY_TRANSFORMATION.forEach((transformation, udf) -> {
            PseudoUDFs.supportedDatatypes().forEach(dataType -> {
                String udfName = udfNameOf(transformation, dataType);
                sqlContext.udf().register(udfName, udf, dataType);
                registeredUDFs.add(udfName);
            });
        });
    }

    @Override
    public String toString() {
        return "PseudoContext{" +
          "pseudoOptions=" + pseudoOptions +
          '}';
    }

    public Dataset<Row> apply(Dataset<Row> ds) {
        return transform(ds, PseudoUDFs.Transformation.APPLY);
    }

    public Dataset<Row> restore(Dataset<Row> ds) {
        return transform(ds, PseudoUDFs.Transformation.RESTORE);
    }

    public Set<String> registeredUDFs() {
        return ImmutableSet.copyOf(registeredUDFs);
    }

    // TODO: Error handling. E.g. what if columns does not exist
    private Dataset<Row> transform(Dataset<Row> ds, PseudoUDFs.Transformation transformation) {
        if (pseudoOptions.isPresent()) {
            for (String colName : pseudoOptions.get().columns()) {
                DataType dataType = columnDataType(colName, ds);

                if (PseudoUDFs.isSupportedDataType(dataType)) {
                    String udfName = udfNameOf(transformation, dataType);
                    Column data = functions.col(colName);
                    Column func = functions.lit(pseudoOptions.get().columnFunc(colName).get());
                    ds = ds.withColumn(colName, functions.callUDF(udfName, func, data));
                }
                else {
                    throw new PseudoException("Unsupported column dataType for pseudo apply UDF: " + dataType);
                }
            }
        }

        return ds;
    }

    /** Determine the DataType of a named column in a Dataset */
    private static DataType columnDataType(String colName, Dataset<Row> ds) {
        for (StructField field : ds.schema().fields()) {
            if (colName.equals(field.name())) {
                return field.dataType();
            }
        }
        throw new PseudoException("Unable to determine dataType for column '" + colName + "'. Does the column exist?");
    }

    /** DataType specific UDF name */
    private static String udfNameOf(PseudoUDFs.Transformation trans, DataType dataType) {
        return String.format("pseudo_%s_%s_udf", trans, dataType.typeName()).toLowerCase();
    }

    public static class PseudoException extends RuntimeException {
        public PseudoException(String message) {
            super(message);
        }
    }
}
