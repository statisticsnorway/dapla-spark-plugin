package no.ssb.dapla.spark.plugin;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.ReadAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.WriteAccessTokenResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.FilesystemMetaDataWriter;
import no.ssb.dapla.spark.plugin.metadata.MetaDataWriterFactory;
import no.ssb.dapla.utils.ProtobufJsonUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;

public class GsimDatasource implements RelationProvider, CreatableRelationProvider, DataSourceRegister {
    private static final String SHORT_NAME = "gsim";

    // TODO: Configure via spark config
    private static final Logger log = LoggerFactory.getLogger(GsimDatasource.class);

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        Span span = getSpan(sqlContext, "spark-read");
        try {
            span.log("CreateRelation via read ");
            SparkOptions options = new SparkOptions(parameters);
            final String path = options.getPath();
            if (path.endsWith("*")) {
                return new CatalogRelation(sqlContext, path.substring(0, path.indexOf("*")), span);
            } else {
                return createRelation(sqlContext, options, span);
            }
        } finally {
            span.finish();
        }
    }

    private BaseRelation createRelation(final SQLContext sqlContext, SparkOptions options, Span span) {
        try {
            final String localPath = options.getPath();
            span.setTag("namespace", localPath);
            System.out.println("Leser datasett fra: " + localPath);

            DataAccessClient dataAccessClient = new DataAccessClient(sqlContext.sparkContext().getConf(), span);

            ReadLocationResponse locationResponse = dataAccessClient.readLocation(ReadLocationRequest.newBuilder()
                    .setPath(localPath)
                    .setSnapshot(0) // 0 means latest
                    .build());

            if (!locationResponse.getAccessAllowed()) {
                span.log("User got permission denied");
                throw new RuntimeException("Permission denied");
            }

            String uriString = DatasetUri.of(locationResponse.getParentUri(), localPath, locationResponse.getVersion()).toString();

            span.log("Path to dataset: " + uriString);
            System.out.println("Path til dataset: " + uriString);
            SQLContext isolatedSqlContext = isolatedContext(sqlContext, localPath, locationResponse.getVersion(), null, null, span);
            return new GsimRelation(isolatedSqlContext, uriString);
        } catch (Exception e) {
            logError(span, e);
            throw e;
        }
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        Span span = getSpan(sqlContext, "spark-write");
        try {
            span.log("CreateRelation via write");
            SparkOptions options = new SparkOptions(parameters);
            final String localPath = options.getPath();
            span.setTag("namespace", localPath);
            span.setTag("valuation", options.getValuation());
            span.setTag("state", options.getState());

            SparkContext sparkContext = sqlContext.sparkContext();
            SparkConf conf = sparkContext.getConf();

            DataAccessClient dataAccessClient = new DataAccessClient(conf, span);

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

            String metadataJson = writeLocationResponse.getValidMetadataJson().toStringUtf8();

            DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(metadataJson, DatasetMeta.class);
            DatasetUri pathToNewDataSet = DatasetUri.of(datasetMeta.getParentUri(), datasetMeta.getId().getPath(), datasetMeta.getId().getVersion());

            span.log("writing file(s) to: " + pathToNewDataSet);
            System.out.println("Skriver datasett til: " + pathToNewDataSet);
            SparkSession sparkSession = sqlContext.sparkSession();
            String metadataSignatureBase64 = new String(Base64.getEncoder().encode(writeLocationResponse.getMetadataSignature().toByteArray()), StandardCharsets.UTF_8);
            setUserContext(sparkSession, pathToNewDataSet.getPath(), pathToNewDataSet.getVersion(), "WRITE", metadataJson, metadataSignatureBase64, span);
            MetadataPublisherClient metadataPublisherClient = new MetadataPublisherClient(conf, span);

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

        } catch (Exception e) {
            logError(span, e);
            throw e;
        } finally {
            unsetUserContext(sqlContext.sparkSession());
            span.finish();
        }
    }

    private Span getSpan(SQLContext sqlContext, String operationName) {
        TracerFactory.createTracer(sqlContext.sparkContext().getConf()).ifPresent(GlobalTracer::registerIfAbsent);
        Span span = GlobalTracer.get().buildSpan(operationName).start();
        GlobalTracer.get().activateSpan(span);
        String accessToken = sqlContext.sparkContext().getConf().get(DaplaSparkConfig.SPARK_SSB_ACCESS_TOKEN);
        DecodedJWT decodedJWT = JWT.decode(accessToken);
        String userId = decodedJWT.getClaim("preferred_username").asString();
        span.setTag("user", userId);
        span.setTag("spark-job-id", sqlContext.sparkContext().getLocalProperty("spark.jobGroup.id"));
        return span;
    }

    private static void logError(Span span, Throwable e) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        span.log(new ImmutableMap.Builder().put("event", "error")
                .put("message", e.getMessage()).put("stacktrace", stringWriter.toString()).build());
    }


    /**
     * Creates a new SQLContext with an isolated spark session.
     *
     * @param sqlContext the original SQLContext (which will be the parent context)
     * @param namespace  namespace info that will be added to the isolated context
     * @return the new SQLContext
     */
    private SQLContext isolatedContext(SQLContext sqlContext, String namespace, String version,
                                       String metadataJson, String metadataSignature, Span span) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        setUserContext(sparkSession, namespace, version, "READ", metadataJson, metadataSignature, span);
        return sparkSession.sqlContext();
    }

    private void setUserContext(SparkSession sparkSession, String namespace, String version, String operation,
                                String metadataJson, String metadataSignature, Span span) {
        if (sparkSession.conf().contains(SparkOptions.ACCESS_TOKEN)) {
            System.out.println("Access token already exists");
        }

        DataAccessClient dataAccessClient = new DataAccessClient(sparkSession.sparkContext().getConf(), span);
        if ("READ".equals(operation)) {

            log.debug("Getting read access token");
            System.out.println("Getting read access token");
            ReadAccessTokenResponse readAccessTokenResponse = dataAccessClient.readAccessToken(ReadAccessTokenRequest.newBuilder()
                    .setPath(namespace)
                    .setVersion(version)
                    .build());
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN, readAccessTokenResponse.getAccessToken());
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN_EXP, readAccessTokenResponse.getExpirationTime());

        } else if ("WRITE".equals(operation)) {

            byte[] datasetMetaSignatureBytes = Base64.getDecoder().decode(metadataSignature);
            log.debug("Getting write access token");
            System.out.println("Getting write access token");
            WriteAccessTokenResponse writeAccessTokenResponse = dataAccessClient.writeAccessToken(WriteAccessTokenRequest.newBuilder()
                    .setMetadataJson(ByteString.copyFromUtf8(metadataJson))
                    .setMetadataSignature(ByteString.copyFrom(datasetMetaSignatureBytes))
                    .build());
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN, writeAccessTokenResponse.getAccessToken());
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN_EXP, writeAccessTokenResponse.getExpirationTime());
        }

    }

    private void unsetUserContext(SparkSession sparkSession) {
        sparkSession.conf().unset(SparkOptions.ACCESS_TOKEN);
        sparkSession.conf().unset(SparkOptions.ACCESS_TOKEN_EXP);
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
