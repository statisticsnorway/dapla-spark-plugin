package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.service.SparkServiceClient;
import no.ssb.dapla.spark.plugin.pseudo.PseudoContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
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
        final String namespace = options.getPath();
        System.out.println("Leser datasett fra: " + namespace);
        String userId = getUserId(sqlContext.sparkContext());

        SparkServiceClient sparkServiceClient = new SparkServiceClient(sqlContext.sparkContext().getConf());
        no.ssb.dapla.catalog.protobuf.Dataset dataset = sparkServiceClient.getDataset(userId, namespace);

        final String location = dataset.getLocations(0);
        System.out.println("Fant datasett: " + location);
        SQLContext isolatedSqlContext = isolatedContext(sqlContext, namespace);
        PseudoContext pseudoContext = new PseudoContext(isolatedSqlContext, parameters);
        return new GsimRelation(isolatedSqlContext, location, pseudoContext);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.info("CreateRelation via write {}", parameters);
        SparkOptions options = new SparkOptions(parameters);
        final String namespace = options.getPath();
        System.out.println("Skriver datasett til: " + namespace);

        SparkContext sparkContext = sqlContext.sparkContext();
        SparkConf conf = sparkContext.getConf();
        DaplaSparkConfig daplaSparkConfig = new DaplaSparkConfig(conf);

        String userId = getUserId(sparkContext);
        String host = daplaSparkConfig.getHost();
        String outputPathPrefix = daplaSparkConfig.getOutputOathPrefix();
        String valuation = options.getValuation();
        String state = options.getState();

        SparkServiceClient sparkServiceClient = new SparkServiceClient(conf);
        no.ssb.dapla.catalog.protobuf.Dataset intendToCreateDataset = sparkServiceClient.createDataset(userId, mode, namespace,
                valuation, state);
        String datasetId = intendToCreateDataset.getId().getId();
        final String pathToNewDataSet = getPathToNewDataset(host, outputPathPrefix, datasetId);
        PseudoContext pseudoContext = new PseudoContext(sqlContext, parameters);

        Lock datasetLock = new ReentrantLock();
        datasetLock.lock();
        RuntimeConfig runtimeConfig = data.sparkSession().conf();
        try {
            log.info("writing file(s) to: {}", pathToNewDataSet);
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, false);
            SparkSession sparkSession = sqlContext.sparkSession();
            setUserContext(sparkSession, "write", namespace);
            data = pseudoContext.apply(data);
            // Write to GCS before updating catalog
            data.coalesce(1).write().parquet(pathToNewDataSet);

            no.ssb.dapla.catalog.protobuf.Dataset writeDataset = createWriteDataset(intendToCreateDataset, mode, namespace, valuation, state, pathToNewDataSet);
            sparkServiceClient.writeDataset(writeDataset, userId);

            // For now give more info in Zepplin
            String resultOutputPath = writeDataset.getLocations(0);
            System.out.println(resultOutputPath);
            return new GsimRelation(isolatedContext(sqlContext, namespace), pathToNewDataSet, pseudoContext);
        } finally {
            datasetLock.unlock();
            runtimeConfig.set(DaplaSparkConfig.FS_GS_IMPL_DISABLE_CACHE, true); // are we sure this was true before?
        }
    }

    private String getPathToNewDataset(String host, String outputPrefix, String datasetId) {
        return host + Path.SEPARATOR
                + outputPrefix + Path.SEPARATOR
                + datasetId + Path.SEPARATOR
                + System.currentTimeMillis();
    }

    private no.ssb.dapla.catalog.protobuf.Dataset createWriteDataset(no.ssb.dapla.catalog.protobuf.Dataset dataset, SaveMode mode, String namespace, String valuation, String state, String addLocation) {
        no.ssb.dapla.catalog.protobuf.Dataset.Builder datasetBuilder = no.ssb.dapla.catalog.protobuf.Dataset.newBuilder().mergeFrom(dataset)
                .setId(DatasetId.newBuilder().setId(dataset.getId().getId()).addName(namespace).build())
                .setValuation(no.ssb.dapla.catalog.protobuf.Dataset.Valuation.valueOf(valuation))
                .setState(no.ssb.dapla.catalog.protobuf.Dataset.DatasetState.valueOf(state))
                .addLocations(addLocation);
        if (mode == SaveMode.Overwrite) {
            datasetBuilder.clearLocations();
        }
        datasetBuilder.addLocations(addLocation).build();
        return datasetBuilder.build();
    }

    /**
     * Until we have a proper way to intercept the spark interpreter in Zeppelin and add userId explicitly,
     * we need to extract the userId from job group id (see org.apache.zeppelin.spark.NewSparkInterpreter#interpret)
     */
    String getUserId(SparkContext sparkContext) {
        String jobDescr = sparkContext.getLocalProperty("spark.jobGroup.id");
        Matcher matcher = Pattern.compile("zeppelin\\-((.*))\\-.{9}\\-.*").matcher(jobDescr);
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
        Text service = new Text(DaplaSparkConfig.getHost(conf));
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
