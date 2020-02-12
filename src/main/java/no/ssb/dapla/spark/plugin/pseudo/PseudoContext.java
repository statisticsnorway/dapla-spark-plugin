package no.ssb.dapla.spark.plugin.pseudo;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import no.ssb.dapla.catalog.protobuf.SecretPseudoConfigItem;
import no.ssb.dapla.catalog.protobuf.VarPseudoConfigItem;
import no.ssb.dapla.dlp.pseudo.func.PseudoFuncConfig;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFunc;
import no.ssb.dapla.dlp.pseudo.func.fpe.FpeFuncConfig;
import no.ssb.dapla.service.SecretServiceClient;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;

import javax.swing.text.html.Option;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.APPLY;
import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.TransformationType.RESTORE;
import static no.ssb.dapla.spark.plugin.pseudo.PseudoUDFs.udfNameOf;

public class PseudoContext {

    private final String datasetPath;
    private final Optional<PseudoOptions> pseudoOptions;
    private final SecretServiceClient secretServiceClient = new SecretServiceClient();
    private final Set<String> referencedSecretIds = new HashSet<>();

    public PseudoContext(SQLContext sqlContext, String datasetPath, scala.collection.immutable.Map<String, String> parameters) {
        this.datasetPath = datasetPath;
        this.pseudoOptions = PseudoOptions.parse(parameters);
        if (pseudoOptions.isPresent()) {
            PseudoOptions opts = pseudoOptions.get();
            Set<PseudoFuncConfig> funcConfigs = parsePseudoConfigs(opts);
            referencedSecretIds.addAll(findReferencedSecretIds(funcConfigs));
            boolean createIfNotExist = true; // TODO: If operation is write then true else false
            Set<SecretServiceClient.Secret> secrets = fetchSecrets(referencedSecretIds, createIfNotExist);
            enrichSecrets(funcConfigs, secrets);

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

    Set<SecretServiceClient.Secret> fetchSecrets(Set<String> secretIds, boolean createIfNotExist) {
        return (createIfNotExist)
          ? secretServiceClient.createOrGetDefaultSecrets(datasetPath, secretIds)
          : secretServiceClient.getSecrets(datasetPath);
    }

    private static boolean isWriteContext(SQLContext sqlContext) {
        return true; // TODO: Implement this
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

    public Dataset<Row> apply(Dataset<Row> ds) {
        return transform(ds, APPLY);
    }

    public Dataset<Row> restore(Dataset<Row> ds) {
        return transform(ds, RESTORE);
    }

    private Dataset<Row> transform(Dataset<Row> ds, PseudoUDFs.TransformationType transformation) {
        if (pseudoOptions.isPresent()) {
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


    private static class ReferencedSecret {
        private String secretId;
        private String paramNameForSecretId;
        private String paramNameForSecret;
        private final String type;

        private ReferencedSecret(String secretId, String paramNameForSecretId, String paramNameForSecret, String type) {
            this.secretId = secretId;
            this.paramNameForSecretId = paramNameForSecretId;
            this.paramNameForSecret = paramNameForSecret;
            this.type = type;
        }

        static ReferencedSecret forFpeFuncWithId(String secretId) {
            return new ReferencedSecret(secretId, FpeFuncConfig.Param.KEY_ID, FpeFuncConfig.Param.KEY, "AES256");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReferencedSecret that = (ReferencedSecret) o;
            return Objects.equals(secretId, that.secretId) &&
              Objects.equals(paramNameForSecretId, that.paramNameForSecretId) &&
              Objects.equals(paramNameForSecret, that.paramNameForSecret) &&
              Objects.equals(type, that.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(secretId, paramNameForSecretId, paramNameForSecret, type);
        }
    }
}
