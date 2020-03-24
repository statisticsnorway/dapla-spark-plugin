package no.ssb.dapla.spark.plugin;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.FilesystemMetaDataWriter;
import no.ssb.dapla.spark.plugin.metadata.MetaDataWriterFactory;
import no.ssb.dapla.spark.plugin.token.TokenRefresher;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

import static no.ssb.dapla.spark.plugin.DaplaSparkConfig.*;

public class GsimDatasource implements RelationProvider, CreatableRelationProvider, DataSourceRegister {
    private static final String SHORT_NAME = "gsim";

    // TODO: Configure via spark config
    private static final Logger log = LoggerFactory.getLogger(GsimDatasource.class);
    private static OAuth2Interceptor interceptor;

    // TODO: We could do better:
    //  https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/SparkSessionExtensions.html
    //  https://issues.apache.org/jira/browse/SPARK-24918
    //  https://spark.apache.org/docs/2.4.5/api/java/org/apache/spark/scheduler/SparkListener.html
    public static synchronized Optional<OAuth2Interceptor> getOAuth2Interceptor(final SparkConf conf) {
        if (conf != null) {
            return Optional.of(createOAuth2Interceptor(conf));
        } else {
            return Optional.of(interceptor);
        }
    }

    public static OAuth2Interceptor createOAuth2Interceptor(final SparkConf conf) {
        if (!conf.contains(CONFIG_ROUTER_OAUTH_TOKEN_URL)) {
            throw new IllegalArgumentException(String.format("Missing configuration: %s", CONFIG_ROUTER_OAUTH_TOKEN_URL));
        }
        String credentialFile = conf.get(CONFIG_ROUTER_OAUTH_CREDENTIALS_FILE, "");
        if (credentialFile.isEmpty()) {
            return new OAuth2Interceptor(
                    conf.get(CONFIG_ROUTER_OAUTH_TOKEN_URL, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_ID, null),
                    conf.get(CONFIG_ROUTER_OAUTH_CLIENT_SECRET, null),
                    Boolean.parseBoolean(conf.get(CONFIG_ROUTER_OAUTH_TOKEN_IGNORE_EXPIRY, "false")),
                    conf
            );
        } else {
            return new OAuth2Interceptor(
                    conf.get(CONFIG_ROUTER_OAUTH_TOKEN_URL, null),
                    credentialFile,
                    Boolean.parseBoolean(conf.get(CONFIG_ROUTER_OAUTH_TOKEN_IGNORE_EXPIRY, "false")),
                    conf
            );
        }
    }

    static {
        try {
            SparkSession sparkSession = SparkSession.getActiveSession().get();
            SparkContext currentContext = sparkSession.sparkContext();
            SparkConf conf = currentContext.getConf();
            interceptor = createOAuth2Interceptor(conf);
        } catch (Exception ex) {
            log.error("Could not initialize default OAuth2Interceptor", ex);
        }
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        log.info("CreateRelation via read {}", parameters);
        SparkOptions options = new SparkOptions(parameters);
        final String localPath = options.getPath();
        System.out.println("Leser datasett fra: " + localPath);

        String accessToken = sqlContext.sparkContext().getConf().get(DaplaSparkConfig.SPARK_SSB_ACCESS_TOKEN);
        DecodedJWT decodedJWT = JWT.decode(accessToken);
        String userId = decodedJWT.getClaim("preferred_username").asString();

        DataAccessClient dataAccessClient = new DataAccessClient(sqlContext.sparkContext().getConf());

        ReadLocationResponse locationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                .setPath(localPath)
                .setSnapshot(0) // 0 means latest
                .build());

        if (!locationResponse.getAccessAllowed()) {
            throw new RuntimeException("Permission denied");
        }

        String uriString = DatasetUri.of(locationResponse.getParentUri(), localPath, locationResponse.getVersion()).toString();

        System.out.println("Path til dataset: " + uriString);
        SQLContext isolatedSqlContext = isolatedContext(sqlContext, localPath, locationResponse.getVersion(), userId, null, null);
        return new GsimRelation(isolatedSqlContext, uriString);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.info("CreateRelation via write {}", parameters);
        SparkOptions options = new SparkOptions(parameters);
        final String localPath = options.getPath();

        SparkContext sparkContext = sqlContext.sparkContext();
        SparkConf conf = sparkContext.getConf();

        String accessToken = sqlContext.sparkContext().getConf().get(DaplaSparkConfig.SPARK_SSB_ACCESS_TOKEN);
        DecodedJWT decodedJWT = JWT.decode(accessToken);
        String userId = decodedJWT.getClaim("preferred_username").asString();

        DataAccessClient dataAccessClient = new DataAccessClient(conf);

        long version = Optional.of(options)
                .map(SparkOptions::getVersion)
                .filter(s -> !s.isEmpty())
                .map(Long::valueOf)
                .orElse(System.currentTimeMillis());

        if (options.getValuation() == null) {
            throw new RuntimeException("valuation is missing in options");
        }
        DatasetMeta.Valuation valuation = DatasetMeta.Valuation.valueOf(options.getValuation());
        if (options.getState() == null) {
            throw new RuntimeException("state is missing in options");
        }
        DatasetMeta.DatasetState state = DatasetMeta.DatasetState.valueOf(options.getState());

        WriteLocationResponse writeLocationResponse = dataAccessClient.writeLocation(WriteLocationRequest.newBuilder()
                .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                        .setId(DatasetId.newBuilder()
                                .setPath(localPath)
                                .setVersion(version)
                                .build())
                        .setType(DatasetMeta.Type.BOUNDED)
                        .setValuation(valuation)
                        .setState(state)
                        .build()))
                .build());

        if (!writeLocationResponse.getAccessAllowed()) {
            throw new RuntimeException("Permission denied");
        }

        RuntimeConfig runtimeConfig = data.sparkSession().conf();
        try {
            String metadataJson = writeLocationResponse.getValidMetadataJson().toStringUtf8();

            DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(metadataJson, DatasetMeta.class);
            DatasetUri pathToNewDataSet = DatasetUri.of(datasetMeta.getParentUri(), datasetMeta.getId().getPath(), datasetMeta.getId().getVersion());

            log.info("writing file(s) to: {}", pathToNewDataSet);
            System.out.println("Skriver datasett til: " + pathToNewDataSet);
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
            SparkSession sparkSession = sqlContext.sparkSession();
            String metadataSignatureBase64 = new String(Base64.getEncoder().encode(writeLocationResponse.getMetadataSignature().toByteArray()), StandardCharsets.UTF_8);
            setUserContext(sparkSession, pathToNewDataSet.getPath(), pathToNewDataSet.getVersion(), userId, "WRITE", metadataJson, metadataSignatureBase64);
            MetadataPublisherClient metadataPublisherClient = new MetadataPublisherClient(conf);

            // Write metadata file
            MetaDataWriterFactory.fromSparkSession(sparkSession).create().writeMetadataFile(datasetMeta, writeLocationResponse.getValidMetadataJson());
            // Publish metadata file created event
            metadataPublisherClient.dataChanged(pathToNewDataSet, FilesystemMetaDataWriter.DATASET_META_FILE_NAME);

            // Write to GCS before writing metadata
            data.coalesce(1).write().mode(SaveMode.Append).parquet(pathToNewDataSet.toString());

            // Write metadata signature file
            MetaDataWriterFactory.fromSparkSession(sparkSession).create().writeSignatureFile(datasetMeta, writeLocationResponse.getMetadataSignature());
            // Publish metadata signature file created event, this will be used for validation and signals a "commit" of metadata
            metadataPublisherClient.dataChanged(pathToNewDataSet, FilesystemMetaDataWriter.DATASET_META_SIGNATURE_FILE_NAME);

            return new GsimRelation(sqlContext, pathToNewDataSet.toString(), data.schema());

        } finally {
            unsetUserContext(sqlContext.sparkSession());
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, true); // are we sure this was true before?
        }
    }

    /**
     * Creates a new SQLContext with an isolated spark session.
     *
     * @param sqlContext the original SQLContext (which will be the parent context)
     * @param namespace  namespace info that will be added to the isolated context
     * @param userId     the userId
     * @return the new SQLContext
     */
    private SQLContext isolatedContext(SQLContext sqlContext, String namespace, String version, String userId, String metadataJson, String metadataSignature) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        sparkSession.conf().set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
        setUserContext(sparkSession, namespace, version, userId, "READ", metadataJson, metadataSignature);
        return sparkSession.sqlContext();
    }

    private void setUserContext(SparkSession sparkSession, String namespace, String version, String userId, String operation, String metadataJson, String metadataSignature) {
        if (sparkSession.conf().contains(SparkOptions.CURRENT_NAMESPACE) ||
                sparkSession.conf().contains(SparkOptions.CURRENT_OPERATION)) {
            System.out.println("Current namespace and/or operation already exists");
        }
        sparkSession.conf().set(SparkOptions.CURRENT_NAMESPACE, namespace);
        sparkSession.conf().set(SparkOptions.CURRENT_DATASET_VERSION, version);
        sparkSession.conf().set(SparkOptions.CURRENT_OPERATION, operation);
        sparkSession.conf().set(SparkOptions.CURRENT_USER, userId);
        if (metadataJson != null) {
            sparkSession.conf().set(SparkOptions.CURRENT_DATASET_META_JSON, metadataJson);
        }
        if (metadataSignature != null) {
            sparkSession.conf().set(SparkOptions.CURRENT_DATASET_META_JSON_SIGNATURE, metadataSignature);
        }
    }

    private void unsetUserContext(SparkSession sparkSession) {
        sparkSession.conf().unset(SparkOptions.CURRENT_NAMESPACE);
        sparkSession.conf().unset(SparkOptions.CURRENT_DATASET_VERSION);
        sparkSession.conf().unset(SparkOptions.CURRENT_OPERATION);
        sparkSession.conf().unset(SparkOptions.CURRENT_USER);
        sparkSession.conf().unset(SparkOptions.CURRENT_DATASET_META_JSON);
        sparkSession.conf().unset(SparkOptions.CURRENT_DATASET_META_JSON_SIGNATURE);
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
