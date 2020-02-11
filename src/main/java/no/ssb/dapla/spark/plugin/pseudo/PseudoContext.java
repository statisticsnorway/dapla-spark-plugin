package no.ssb.dapla.spark.plugin.pseudo;

import no.ssb.dapla.catalog.protobuf.PseudoConfigItem;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFunc;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.util.Json;
import no.ssb.dapla.service.SecretServiceClient;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import scala.collection.immutable.Map;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.APPLY;
import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.RESTORE;
import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.udfNameOf;

public class PseudoContext {

    private final Optional<PseudoOptions> pseudoOptions;
    private final SecretServiceClient secretServiceClient = new SecretServiceClient();

    public PseudoContext(SQLContext sqlContext, Map<String, String> parameters) {
        this.pseudoOptions = PseudoOptions.parse(parameters);

        if (pseudoOptions.isPresent()) {
            PseudoOptions opts = pseudoOptions.get();
            Set<PseudoFuncConfig> funcConfigs = parsePseudoConfigs(opts);

            funcConfigs.forEach(funcConfig -> {
                Optional<String> keyId = funcConfig.get(FpeFuncConfig.Param.KEY_ID, String.class);
                if (keyId.isPresent() && FpeFunc.class.getName().equals(funcConfig.getFuncImpl())) {
                    SecretServiceClient.Secret secret = secretServiceClient.createOrGetSecret(keyId.get(), "AES256");
                    funcConfig.add(FpeFuncConfig.Param.KEY, secret.getBase64EncodedBody());
                }
            });

            PseudoUDFs.registerUDFs(sqlContext, funcConfigs);
        }
    }

    private static Set<PseudoFuncConfig> parsePseudoConfigs(PseudoOptions opts) {
        return opts.functions().stream()
          .map(PseudoFuncConfigFactory::get)
          .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "PseudoContext{" +
          "pseudoOptions=" + pseudoOptions +
          '}';
    }

    public Iterable<PseudoConfigItem> getPseudoConfigItems() {
        if (pseudoOptions.isPresent()) {
            PseudoOptions opts = pseudoOptions.get();
            return opts.columns().stream()
              .map(col -> PseudoConfigItem.newBuilder()
                .setCol(col)
                .setPseudoFunc(opts.columnFunc(col).orElse(""))
                .build())
              .collect(Collectors.toList());
        }
        else {
            return Collections.emptyList();
        }
    }

    public String getConfigJson() {
        return pseudoOptions.isPresent() ? pseudoOptions.get().toJson() : "[]";
    }

    public Dataset<Row> apply(Dataset<Row> ds) {
        return transform(ds, APPLY);
    }

    public Dataset<Row> restore(Dataset<Row> ds) {
        return transform(ds, RESTORE);
    }

    private Dataset<Row> transform(Dataset<Row> ds, PseudoUDFs.TransformationType transformation) {
        if (pseudoOptions.isPresent()) {
            final PseudoOptions opts = this.pseudoOptions.get();
            for (String colName : opts.columns()) {
                String colFunc = opts.columnFunc(colName).orElseThrow( () -> new PseudoException("Missing pseudoFunc for col " + colName));
                DataType dataType = columnDataType(colName, ds);

                if (PseudoUDFs.isSupportedDataType(dataType)) {
                    String udfName = udfNameOf(colFunc, transformation, dataType);
                    Column data = functions.col(colName);
                    ds = ds.withColumn(colName, functions.callUDF(udfName, data));
                }
                else {
                    throw new PseudoException("Unsupported column dataType for pseudo UDF: " + dataType);
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

}
