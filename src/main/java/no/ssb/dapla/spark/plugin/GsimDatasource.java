package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.data.access.protobuf.LocationResponse;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.service.DataAccessClient;
import no.ssb.dapla.spark.plugin.metadata.MetaDataWriterFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
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

        // TODO: use a util for this
        String fullPath = locationResponse.getParentUri() + "/" + localPath + "/" + locationResponse.getVersion();

        System.out.println("Path til dataset: " + fullPath);
        SQLContext isolatedSqlContext = isolatedContext(sqlContext, localPath);
        return new GsimRelation(isolatedSqlContext, fullPath);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.info("CreateRelation via write {}", parameters);
        SparkOptions options = new SparkOptions(parameters);
        final String localPath = options.getPath();
        System.out.println("Skriver datasett til: " + localPath);

        SparkContext sparkContext = sqlContext.sparkContext();
        SparkConf conf = sparkContext.getConf();

        String userId = getUserId(sparkContext);

        DataAccessClient dataAccessClient = new DataAccessClient(conf);
        long currentTimeStamp = System.currentTimeMillis();
        LocationResponse location = dataAccessClient.getLocation(userId, localPath, currentTimeStamp);

        final String pathToNewDataSet = getPathToNewDataset(location.getParentUri(), localPath, currentTimeStamp);

        Lock datasetLock = new ReentrantLock();
        datasetLock.lock();
        RuntimeConfig runtimeConfig = data.sparkSession().conf();
        try {
            log.info("writing file(s) to: {}", pathToNewDataSet);
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
            SparkSession sparkSession = sqlContext.sparkSession();
            setUserContext(sparkSession, "write", localPath);
            // Write to GCS before writing metadata
            data.coalesce(1).write().parquet(pathToNewDataSet);

            // TODO: This should noe be written to the bucket as a metadata file
            DatasetMeta datasetMeta = DatasetMeta.newBuilder()
                    .setId(DatasetId.newBuilder()
                            .setPath(localPath)
                            .setVersion(currentTimeStamp)
                            .build())
                    .setType(DatasetMeta.Type.BOUNDED)
                    .setValuation(DatasetMeta.Valuation.valueOf(options.getValuation()))
                    .setState(DatasetMeta.DatasetState.valueOf(options.getState()))
                    .setParentUri(location.getParentUri())
                    .setCreatedBy("todo") // TODO: userid
                    .build();

            MetaDataWriterFactory.fromSparkConf(conf)
                    .create()
                    .write(datasetMeta);

            // For now give more info in Zepplin
            System.out.println("Path to dataset:" + pathToNewDataSet);
            return new GsimRelation(isolatedContext(sqlContext, localPath), pathToNewDataSet);
        } catch (IOException e) {
            log.error("Could not write meta-data to bucket", e);
            throw new RuntimeException(e);
        } finally {
            datasetLock.unlock();
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, true); // are we sure this was true before?
        }
    }

    private String getPathToNewDataset(String parentUri, String path, long version) {
        return parentUri + Path.SEPARATOR
                + path + Path.SEPARATOR
                + version;
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
     * @return the new SQLContext
     */
    private SQLContext isolatedContext(SQLContext sqlContext, String namespace) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        sparkSession.conf().set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
        setUserContext(sparkSession, "read", namespace);
        return sparkSession.sqlContext();
    }

    // TODO: This should only be set when the user has access to the current operation and namespace
    private void setUserContext(SparkSession sparkSession, String operation, String namespace) {
        SparkConf conf = sparkSession.sparkContext().getConf();
        Text service = new Text(DaplaSparkConfig.getStoragePath(conf));
        sparkSession.conf().set(BrokerTokenIdentifier.CURRENT_NAMESPACE, namespace);
        sparkSession.conf().set(BrokerTokenIdentifier.CURRENT_OPERATION, operation);
        try {
            UserGroupInformation.getCurrentUser().addToken(service,
                    BrokerDelegationTokenBinding.createUserToken(service, new Text(operation), new Text(namespace)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
