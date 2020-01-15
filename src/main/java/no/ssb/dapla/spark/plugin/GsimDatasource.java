package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import no.ssb.dapla.spark.plugin.pseudo.PseudoContext;
import no.ssb.dapla.spark.router.DataLocation;
import no.ssb.dapla.spark.router.SparkServiceRouter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
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
import scala.Option;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;
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
        log.debug("CreateRelation via read {}", parameters);
        System.out.println("Leser datasett fra: " + getNamespace(parameters));
        String bucket = getBucket(sqlContext.sparkContext());
        String userId = getUserId(sqlContext.sparkContext());
        PseudoContext pseudoContext = new PseudoContext(sqlContext, parameters);

        DataLocation location = SparkServiceRouter.getInstance(bucket).read(userId, getNamespace(parameters));
        return new GsimRelation(isolatedContext(sqlContext, location), location.getPaths(), pseudoContext);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.debug("CreateRelation via write {}", parameters);
        System.out.println("Skriver datasett til: " + getNamespace(parameters));

        String userId = getUserId(sqlContext.sparkContext());
        String bucket = getBucket(sqlContext.sparkContext());
        String dataId = bucket + "/" + UUID.randomUUID() + ".parquet";
        PseudoContext pseudoContext = new PseudoContext(sqlContext, parameters);

        DataLocation location = SparkServiceRouter.getInstance(bucket).write(mode, userId, getNamespace(parameters), dataId);
        URI newDataUri = location.getPaths().get(0);
        Lock datasetLock = new ReentrantLock();
        datasetLock.lock();
        try {
            log.debug("writing file(s) to: {}", newDataUri);
            data.sparkSession().conf().set("fs.gs.impl.disable.cache", "false");
            setUserContext(sqlContext.sparkSession(), "write", location);
            data = pseudoContext.apply(data);
            data.coalesce(1).write().parquet(newDataUri.toASCIIString());
            return new GsimRelation(isolatedContext(sqlContext, location), location.getPaths(), pseudoContext);
        } finally {
            datasetLock.unlock();
            data.sparkSession().conf().set("fs.gs.impl.disable.cache", "true");
        }
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
     * @param location   location info that will be added to the context
     * @return the new SQLContext
     */
    private SQLContext isolatedContext(SQLContext sqlContext, DataLocation location) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        sparkSession.conf().set("fs.gs.impl.disable.cache", "false");
        setUserContext(sparkSession, "read", location);
        return sparkSession.sqlContext();
    }

    // TODO: This should only be set when the user has access to the current operation and namespace
    private void setUserContext(SparkSession sparkSession, String operation, DataLocation location) {
        Text service = new Text(location.getLocation());
        sparkSession.conf().set(BrokerTokenIdentifier.CURRENT_NAMESPACE, location.getNamespace());
        sparkSession.conf().set(BrokerTokenIdentifier.CURRENT_OPERATION, operation);
        try {
            UserGroupInformation.getCurrentUser().addToken(service,
                    BrokerDelegationTokenBinding.createUserToken(service, new Text(operation), new Text(location.getNamespace())));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getBucket(SparkContext sparkContext) {
        return sparkContext.getConf().get("spark.ssb.dapla.gcs.storage");
    }

    private String getNamespace(Map<String, String> parameters) {
        Option<String> path = parameters.get("PATH");
        if (path.isEmpty()) {
            throw new IllegalStateException("PATH missing from parameters" + parameters);
        }
        return path.get();
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
