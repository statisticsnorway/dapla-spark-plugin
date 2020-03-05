package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.data.access.protobuf.DatasetState;
import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.data.access.protobuf.Privilege;
import no.ssb.dapla.data.access.protobuf.Valuation;
import no.ssb.dapla.data.access.protobuf.WriteOptions;
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

import static java.util.Optional.ofNullable;

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

        String userId = sqlContext.sparkContext().getConf().get("spark.ssb.username");
        String userAccessToken = sqlContext.sparkContext().getConf().get("spark.ssb.access");

        DataAccessClient dataAccessClient = new DataAccessClient(sqlContext.sparkContext().getConf());

        LocationResponse locationResponse = dataAccessClient.getReadLocationWithLatestVersion(userAccessToken, localPath);

        if (!locationResponse.getAccessAllowed()) {
            throw new RuntimeException("Permission denied");
        }

        String fullPath = DatasetUri.of(locationResponse.getParentUri(), localPath, locationResponse.getVersion()).toString();

        System.out.println("Path til dataset: " + fullPath);
        SQLContext isolatedSqlContext = isolatedContext(sqlContext, localPath, userId, null, null);
        return new GsimRelation(isolatedSqlContext, fullPath);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.info("CreateRelation via write {}", parameters);
        SparkOptions options = new SparkOptions(parameters);
        final String localPath = options.getPath();
        Valuation valuation = Valuation.valueOf(options.getValuation());
        DatasetState state = DatasetState.valueOf(options.getState());
        WriteOptions writeOptions = WriteOptions.newBuilder().setValuation(valuation).setState(state).build();

        SparkContext sparkContext = sqlContext.sparkContext();
        SparkConf conf = sparkContext.getConf();

        String userId = sqlContext.sparkContext().getConf().get("spark.ssb.username");
        String userAccessToken = sqlContext.sparkContext().getConf().get("spark.ssb.access");

        DataAccessClient dataAccessClient = new DataAccessClient(conf);

        LocationResponse locationResponse = dataAccessClient.getWriteLocation(userAccessToken, localPath, writeOptions);

        if (!locationResponse.getAccessAllowed()) {
            throw new RuntimeException("Permission denied");
        }

        final DatasetUri pathToNewDataSet = DatasetUri.of(locationResponse.getParentUri(), localPath, System.currentTimeMillis());

        Lock datasetLock = new ReentrantLock();
        datasetLock.lock();
        RuntimeConfig runtimeConfig = data.sparkSession().conf();
        try {
            log.info("writing file(s) to: {}", pathToNewDataSet);
            System.out.println("Skriver datasett til: " + pathToNewDataSet);
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
            SparkSession sparkSession = sqlContext.sparkSession();
            setUserContext(sparkSession, Privilege.WRITE, pathToNewDataSet.getPath(), userId, valuation, state);
            // Write to GCS before writing metadata
            data.coalesce(1).write().parquet(pathToNewDataSet.toString());

            DatasetMeta datasetMeta = DatasetMeta.newBuilder()
                    .setId(DatasetId.newBuilder()
                            .setPath(pathToNewDataSet.getPath())
                            .setVersion(Long.parseLong(pathToNewDataSet.getVersion()))
                            .build())
                    .setType(DatasetMeta.Type.BOUNDED)
                    .setValuation(DatasetMeta.Valuation.valueOf(options.getValuation()))
                    .setState(DatasetMeta.DatasetState.valueOf(options.getState()))
                    .setParentUri(pathToNewDataSet.getParentUri())
                    .setCreatedBy(userId)
                    .build();

            MetaDataWriterFactory.fromSparkContext(sparkContext)
                    .create()
                    .write(datasetMeta);

            // Publish that new data has arrived
            MetadataPublisherClient metadataPublisherClient = new MetadataPublisherClient(conf);
            metadataPublisherClient.dataChanged(userAccessToken, pathToNewDataSet);

        } catch (IOException e) {
            log.error("Could not write meta-data to bucket", e);
            throw new RuntimeException(e);
        } finally {
            datasetLock.unlock();
            unsetUserContext(sqlContext.sparkSession());
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, true); // are we sure this was true before?
        }
        return new GsimRelation(isolatedContext(sqlContext, pathToNewDataSet.getPath(), userId, valuation, state), pathToNewDataSet.toString(), data.schema());
    }

    /**
     * Creates a new SQLContext with an isolated spark session.
     *
     * @param sqlContext the original SQLContext (which will be the parent context)
     * @param namespace  namespace info that will be added to the isolated context
     * @param userId     the userId
     * @return the new SQLContext
     */
    private SQLContext isolatedContext(SQLContext sqlContext, String namespace, String userId, Valuation valuation, DatasetState state) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        sparkSession.conf().set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
        setUserContext(sparkSession, Privilege.READ, namespace, userId, valuation, state);
        return sparkSession.sqlContext();
    }

    // TODO: This should only be set when the user has access to the current operation and namespace
    private void setUserContext(SparkSession sparkSession, Privilege privilege,
                                String namespace, String userId,
                                Valuation valuation, DatasetState state) {
        if (sparkSession.conf().contains(SparkOptions.CURRENT_NAMESPACE) ||
                sparkSession.conf().contains(SparkOptions.CURRENT_OPERATION)) {
            System.out.println("Current namespace and/or operation already exists");
        }
        sparkSession.conf().set(SparkOptions.CURRENT_NAMESPACE, namespace);
        sparkSession.conf().set(SparkOptions.CURRENT_OPERATION, privilege.name());
        sparkSession.conf().set(SparkOptions.CURRENT_USER, userId);
        sparkSession.conf().set(SparkOptions.CURRENT_DATASET_VALUATION, ofNullable(valuation).map(Valuation::name).orElse(""));
        sparkSession.conf().set(SparkOptions.CURRENT_DATASET_STATE, ofNullable(state).map(DatasetState::name).orElse(""));
    }

    private void unsetUserContext(SparkSession sparkSession) {
        sparkSession.conf().unset(SparkOptions.CURRENT_NAMESPACE);
        sparkSession.conf().unset(SparkOptions.CURRENT_OPERATION);
        sparkSession.conf().unset(SparkOptions.CURRENT_USER);
        sparkSession.conf().unset(SparkOptions.CURRENT_DATASET_VALUATION);
        sparkSession.conf().unset(SparkOptions.CURRENT_DATASET_STATE);
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
