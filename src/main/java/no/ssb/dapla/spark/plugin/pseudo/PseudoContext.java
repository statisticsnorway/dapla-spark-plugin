package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.catalog.protobuf.SecretPseudoConfigItem;
import no.ssb.dapla.catalog.protobuf.VarPseudoConfigItem;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFunc;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFuncConfig;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.service.SecretServiceClient;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.APPLY;
import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.RESTORE;
import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.udfNameOf;

public class PseudoContext {

    private final no.ssb.dapla.catalog.protobuf.Dataset datasetMeta;
    private final Optional<PseudoOptions> pseudoOptions;
    private final SecretServiceClient secretServiceClient = new SecretServiceClient();
    private final Set<String> referencedSecretIds = new HashSet<>();
    private final SQLContext sqlContext;
    private final String operationType;

    public PseudoContext(SQLContext sqlContext, no.ssb.dapla.catalog.protobuf.Dataset datasetMeta, scala.collection.immutable.Map<String, String> parameters, String operationType) {
        this.sqlContext = sqlContext;
        this.datasetMeta = datasetMeta;
        this.operationType = operationType; // TODO: Determine this from SQLContext
        this.pseudoOptions = PseudoOptions.parse(parameters);
        if (pseudoOptions.isPresent()) {
            PseudoOptions opts = pseudoOptions.get();
            Set<PseudoFuncConfig> funcConfigs = (isWriteOperation())
              ? parseWritePseudoConfigs(opts)
              : parseReadPseudoConfigs(opts, datasetMeta.getPseudoConfig()) ;

            referencedSecretIds.addAll(findReferencedSecretIds(funcConfigs));
            Set<SecretServiceClient.Secret> secrets = fetchSecrets(referencedSecretIds);
            enrichSecrets(funcConfigs, secrets);

            PseudoUDFs.registerUDFs(sqlContext, funcConfigs);
        }
    }

    private static Set<PseudoFuncConfig> parseWritePseudoConfigs(PseudoOptions opts) {
        // TODO: Validate that function declaration is valid
        return opts.functions().stream()
          .map(PseudoFuncConfigFactory::get)
          .collect(Collectors.toSet());
    }

    private static Set<PseudoFuncConfig> parseReadPseudoConfigs(PseudoOptions opts, PseudoConfig catalogPseudoConfig) {
        Map<String, String> catalogVarToFuncMap = catalogPseudoConfig.getVarsList().stream()
          .collect(Collectors.toMap(VarPseudoConfigItem::getVar, VarPseudoConfigItem::getPseudoFunc));

        // TODO: Consistency validation, err out if mismatch beteween catalog and supplied options
        return opts.vars().stream()
          .map(var -> {
              String pseudoFuncDecl = Optional.ofNullable(catalogVarToFuncMap.get(var)).orElseThrow(() -> new PseudoException("Could not find pseudo config for variable " + var + " in catalog"));
              return PseudoFuncConfigFactory.get(pseudoFuncDecl);
          })
          .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "PseudoContext{" +
          "pseudoOptions=" + pseudoOptions +
          '}';
    }

    Set<SecretServiceClient.Secret> fetchSecrets(Set<String> secretIds) {
        return (isWriteOperation())
          ? secretServiceClient.createOrGetDefaultSecrets(datasetPath(), secretIds)
          : secretServiceClient.getSecrets(datasetPath());
    }

    public boolean isReadOperation() {
        return "read".equals(this.operationType);
        // return "read".equals(operationTypeOf(sqlContext));
    }

    public boolean isWriteOperation() {
        return "write".equals(this.operationType);
        // return "write".equals(operationTypeOf(sqlContext));
    }

    private static Set<String> findReferencedSecretIds(Collection<PseudoFuncConfig> funcConfigs) {
        Set<String> refSecs = new HashSet<>();
        funcConfigs.stream().forEach(funcConfig -> {
            Optional<String> keyId = funcConfig.get(FpeFuncConfig.Param.KEY_ID, String.class);
            if (keyId.isPresent() && FpeFunc.class.getName().equals(funcConfig.getFuncImpl())) {
                refSecs.add(keyId.get());
            }
        });
        return refSecs;
    }

    private static void enrichSecrets(Collection<PseudoFuncConfig> funcConfigs, Iterable<SecretServiceClient.Secret> secrets) {
        final Map<String, SecretServiceClient.Secret> secretsMap =  Maps.uniqueIndex(secrets, new Function<SecretServiceClient.Secret, String>() {
            public String apply(SecretServiceClient.Secret secret) {
                return secret.getId();
            }
        });

        funcConfigs.stream().forEach(funcConfig -> {
            Optional<String> keyId = funcConfig.get(FpeFuncConfig.Param.KEY_ID, String.class);

            if (keyId.isPresent() && FpeFunc.class.getName().equals(funcConfig.getFuncImpl())) {
                SecretServiceClient.Secret secret = Optional.ofNullable(secretsMap.get(keyId.get())).orElseThrow(() ->
                  new PseudoException("No secret found for FPE keyId=" + keyId));
                funcConfig.add(FpeFuncConfig.Param.KEY, secret.getBase64EncodedBody());
            }
        });
    }

    public Optional<PseudoConfig> getCatalogPseudoConfig() {
        if (pseudoOptions.isPresent()) {
            PseudoOptions opts = pseudoOptions.get();
            PseudoConfig.Builder pseudoConfig = PseudoConfig.newBuilder()
              .addAllVars(
                opts.vars().stream().map(var -> VarPseudoConfigItem.newBuilder()
                  .setVar(var)
                  .setPseudoFunc(opts.pseudoFunc(var).orElse(""))
                  .build())
                  .collect(Collectors.toList()
                  ))
              .addAllSecrets(referencedSecretIds.stream().map(secretId -> SecretPseudoConfigItem.newBuilder()
                .setId(secretId)
                .build())
                .collect(Collectors.toList())
              );

            return Optional.of(pseudoConfig.build());
        }
        else {
            return Optional.empty();
        }
    }

    public String getConfigJson() {
        return pseudoOptions.isPresent() ? pseudoOptions.get().toJson() : "[]";
    }

    private String datasetPath() {
        return String.join("/",  datasetMeta.getId().getNameList());
    }

    public Dataset<Row> apply(Dataset<Row> ds) {
        if (isReadOperation()) {
            throw new PseudoException("Illegal pseudo context state. Cannot pseudonymize in a read-only context.");
        }
        return transform(ds, APPLY);
    }

    public Dataset<Row> restore(Dataset<Row> ds) {
        if (isWriteOperation()) {
            throw new PseudoException("");
        }
        return transform(ds, RESTORE);
    }

    private Dataset<Row> transform(Dataset<Row> ds, PseudoUDFs.TransformationType transformation) {
        if (pseudoOptions.isPresent()) {

            if (transformation == RESTORE && isWriteOperation()) {
                throw new PseudoException("Illegal pseudo context state. Cannot depseudonymize in a write context.");
            }
            else if (transformation == APPLY && isReadOperation()) {
                throw new PseudoException("Illegal pseudo context state. Cannot pseudonymize in a read context.");
            }

            final PseudoOptions opts = this.pseudoOptions.get();
            for (String var : opts.vars()) {
                String funcDecl = opts.pseudoFunc(var).orElseThrow( () -> new PseudoException("Missing pseudoFunc for variable " + var));
                DataType dataType = columnDataType(var, ds);

                if (PseudoUDFs.isSupportedDataType(dataType)) {
                    String udfName = udfNameOf(funcDecl, transformation, dataType);
                    Column data = functions.col(var);
                    ds = ds.withColumn(var, functions.callUDF(udfName, data));
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

    /**
     * Inspect SQLContext and return current operation. Assumes that the current operation has been specified in
     * advance, e.g. by the Datasource.
     *
     * @throws PseudoException if current operation type could not be determined
     */
    private static String operationTypeOf(SQLContext sqlContext) {
        try {
            return sqlContext.sparkSession().conf().get(BrokerTokenIdentifier.CURRENT_OPERATION).toLowerCase();
        }
        catch (Exception e) {
            throw new PseudoException("Unable to determine pseudo operation from current spark session", e);
        }
    }

}
