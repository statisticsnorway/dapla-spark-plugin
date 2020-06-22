package no.ssb.dapla.spark.plugin;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.collect.ImmutableMap;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;
import no.ssb.dapla.data.access.protobuf.ReadLocationRequest;
import no.ssb.dapla.data.access.protobuf.ReadLocationResponse;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.FilesystemMetaDataWriter;
import no.ssb.dapla.spark.plugin.metadata.MetaDataWriter;
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
            PathValidator.validateRead(path);
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
            SQLContext isolatedSqlContext = isolatedContext(sqlContext, locationResponse.getAccessToken(), locationResponse.getExpirationTime());
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
            PathValidator.validateWrite(localPath);
            span.setTag("namespace", localPath);
            span.setTag("valuation", options.getValuation());
            span.setTag("state", options.getState());

            SparkContext sparkContext = sqlContext.sparkContext();
            SparkConf conf = sparkContext.getConf();

            DataAccessClient dataAccessClient = new DataAccessClient(conf, span);

            String version = Optional.of(options)
                    .map(SparkOptions::getVersion)
                    .filter(s -> !s.isEmpty())
                    .orElse(String.valueOf(System.currentTimeMillis()));

            if (options.getValuation() == null) {
                throw new RuntimeException("valuation is missing in options");
            }
            Valuation valuation = Valuation.valueOf(options.getValuation());
            if (options.getState() == null) {
                throw new RuntimeException("state is missing in options");
            }
            DatasetState state = DatasetState.valueOf(options.getState());

            WriteLocationResponse writeLocationResponse = dataAccessClient.writeLocation(WriteLocationRequest.newBuilder()
                    .setMetadataJson(ProtobufJsonUtils.toString(DatasetMeta.newBuilder()
                            .setId(DatasetId.newBuilder()
                                    .setPath(localPath)
                                    .setVersion(version)
                                    .build())
                            .setType(Type.BOUNDED)
                            .setValuation(valuation)
                            .setState(state)
                            .build()))
                    .build());

            if (!writeLocationResponse.getAccessAllowed()) {
                throw new RuntimeException("Permission denied");
            }

            String metadataJson = writeLocationResponse.getValidMetadataJson().toStringUtf8();

            String parentUri = writeLocationResponse.getParentUri();
            DatasetMeta datasetMeta = ProtobufJsonUtils.toPojo(metadataJson, DatasetMeta.class);
            DatasetUri pathToNewDataSet = DatasetUri.of(parentUri, datasetMeta.getId().getPath(), datasetMeta.getId().getVersion());

            span.log("writing file(s) to: " + pathToNewDataSet);
            System.out.println("Skriver datasett til: " + pathToNewDataSet);
            SparkSession sparkSession = sqlContext.sparkSession();
            setUserContext(sparkSession, writeLocationResponse.getAccessToken(), writeLocationResponse.getExpirationTime());
            MetadataPublisherClient metadataPublisherClient = new MetadataPublisherClient(conf, span);

            // Write metadata file
            final MetaDataWriter metaDataWriter = MetaDataWriterFactory.fromSparkSession(sparkSession).create();
            metaDataWriter.writeMetadataFile(parentUri, datasetMeta, writeLocationResponse.getValidMetadataJson());
            // Publish metadata file created event
            metadataPublisherClient.dataChanged(pathToNewDataSet, FilesystemMetaDataWriter.DATASET_META_FILE_NAME);

            // Write to GCS before writing metadata
            data.write().mode(SaveMode.Append).parquet(pathToNewDataSet.toString());

            // Write schema doc
            if (options.getDoc() != null) {
                metaDataWriter.writeSchemaFile(parentUri, datasetMeta, options.getDoc());
            }

            // Write metadata signature file
            metaDataWriter.writeSignatureFile(parentUri, datasetMeta, writeLocationResponse.getMetadataSignature());
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
     * @param accessToken  namespace info that will be added to the isolated context
     * @param expirationTime  namespace info that will be added to the isolated context
     * @return the new SQLContext
     */
    private SQLContext isolatedContext(SQLContext sqlContext, String accessToken, long expirationTime) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        setUserContext(sparkSession, accessToken, expirationTime);
        return sparkSession.sqlContext();
    }

    private void setUserContext(SparkSession sparkSession, String accessToken, long expirationTime) {
        if (sparkSession.conf().contains(SparkOptions.ACCESS_TOKEN)) {
            System.out.println("Access token already exists");
        }
        if (accessToken != null) {
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN, accessToken);
            sparkSession.conf().set(SparkOptions.ACCESS_TOKEN_EXP, expirationTime);
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
