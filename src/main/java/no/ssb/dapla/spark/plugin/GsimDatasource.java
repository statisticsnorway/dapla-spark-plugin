package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.data.access.protobuf.AccessTokenRequest;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.service.MetadataPublisherClient;
import no.ssb.dapla.spark.plugin.metadata.MetaDataWriterFactory;
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


import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GsimDatasource implements RelationProvider, CreatableRelationProvider, DataSourceRegister {
    private static final String SHORT_NAME = "gsim";

    // TODO: Configure via spark config
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        log.info("CreateRelation via read {}", parameters);
        SparkOptions options = new SparkOptions(parameters);
        final String localPath = options.getPath();
        System.out.println("Leser datasett fra: " + localPath);
        String userId = getUserId(sqlContext.sparkContext());

        DataAccessClient dataAccessClient = new DataAccessClient(sqlContext.sparkContext().getConf());
        LocationResponse locationResponse = dataAccessClient.getLocationWithLatestVersion(userId, localPath);

        String fullPath = DatasetUri.of(locationResponse.getParentUri(), localPath, locationResponse.getVersion()).toString();

        System.out.println("Path til dataset: " + fullPath);
        SQLContext isolatedSqlContext = isolatedContext(sqlContext, localPath, userId);
        return new GsimRelation(isolatedSqlContext, fullPath);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.info("CreateRelation via write {}", parameters);
        SparkOptions options = new SparkOptions(parameters);
        final String localPath = options.getPath();

        SparkContext sparkContext = sqlContext.sparkContext();
        SparkConf conf = sparkContext.getConf();

        String userId = getUserId(sparkContext);

        DataAccessClient dataAccessClient = new DataAccessClient(conf);
        long currentTimeStamp = System.currentTimeMillis();
        LocationResponse location = dataAccessClient.getLocation(userId, localPath, currentTimeStamp);

        final String pathToNewDataSet = DatasetUri.of(location.getParentUri(), localPath, currentTimeStamp).toString();

        Lock datasetLock = new ReentrantLock();
        datasetLock.lock();
        RuntimeConfig runtimeConfig = data.sparkSession().conf();
        try {
            log.info("writing file(s) to: {}", pathToNewDataSet);
            System.out.println("Skriver datasett til: " + pathToNewDataSet);
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
            SparkSession sparkSession = sqlContext.sparkSession();
            setUserContext(sparkSession, AccessTokenRequest.Privilege.WRITE, localPath, userId);
            // Write to GCS before writing metadata
            data.coalesce(1).write().parquet(pathToNewDataSet);

            DatasetMeta datasetMeta = DatasetMeta.newBuilder()
                    .setId(DatasetId.newBuilder()
                            .setPath(localPath)
                            .setVersion(currentTimeStamp)
                            .build())
                    .setType(DatasetMeta.Type.BOUNDED)
                    .setValuation(DatasetMeta.Valuation.valueOf(options.getValuation()))
                    .setState(DatasetMeta.DatasetState.valueOf(options.getState()))
                    .setParentUri(location.getParentUri())
                    .setCreatedBy(userId)
                    .build();

            MetaDataWriterFactory.fromSparkConf(conf)
                    .create()
                    .write(datasetMeta);

            // Publish that new data has arrived
            MetadataPublisherClient metadataPublisherClient = new MetadataPublisherClient(conf);
            metadataPublisherClient.dataChanged(location.getParentUri(), localPath, currentTimeStamp);

        } catch (IOException e) {
            log.error("Could not write meta-data to bucket", e);
            throw new RuntimeException(e);
        } finally {
            datasetLock.unlock();
            unsetUserContext(sqlContext.sparkSession());
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, true); // are we sure this was true before?
        }
        return new GsimRelation(isolatedContext(sqlContext, localPath, userId), pathToNewDataSet);
    }

    /**
     * Until we have a proper way to intercept the spark interpreter in Zeppelin and add userId explicitly,
     * we need to extract the userId from job group id (see org.apache.zeppelin.spark.NewSparkInterpreter#interpret)
     */
    String getUserId(SparkContext sparkContext) {
        String jobDescr = sparkContext.getLocalProperty("spark.jobGroup.id");
        Matcher matcher = Pattern.compile("zeppelin-((?:[^-]+)|(?:[^@]+@[^-]+))-[^@]*-[^-]{8}-[^_]{6}_[0-9]+").matcher(jobDescr);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            // Fallback when running locally
            return sparkContext.getConf().get("spark.ssb.user");
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
    private SQLContext isolatedContext(SQLContext sqlContext, String namespace, String userId) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        sparkSession.conf().set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
        setUserContext(sparkSession, AccessTokenRequest.Privilege.READ, namespace, userId);
        return sparkSession.sqlContext();
    }

    // TODO: This should only be set when the user has access to the current operation and namespace
    private void setUserContext(SparkSession sparkSession, AccessTokenRequest.Privilege privilege,
                                String namespace, String userId) {
        if (sparkSession.conf().contains(SparkOptions.CURRENT_NAMESPACE) ||
                sparkSession.conf().contains(SparkOptions.CURRENT_OPERATION)) {
            System.out.println("Current namespace and/or operation already exists");
        }
        sparkSession.conf().set(SparkOptions.CURRENT_NAMESPACE, namespace);
        sparkSession.conf().set(SparkOptions.CURRENT_OPERATION, privilege.name());
        sparkSession.conf().set(SparkOptions.CURRENT_USER, userId);
    }

    private void unsetUserContext(SparkSession sparkSession) {
        sparkSession.conf().unset(SparkOptions.CURRENT_NAMESPACE);
        sparkSession.conf().unset(SparkOptions.CURRENT_OPERATION);
        sparkSession.conf().unset(SparkOptions.CURRENT_USER);
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
