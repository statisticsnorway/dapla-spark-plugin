package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.gcs.token.delegation.BrokerDelegationTokenBinding;
import no.ssb.dapla.gcs.token.delegation.BrokerTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GsimDatasource implements RelationProvider, CreatableRelationProvider, DataSourceRegister {
    private static final String SHORT_NAME = "gsim";
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        log.debug("CreateRelation via read {}", parameters);
        System.out.println("CreateRelation via read - " + parameters);
        System.out.println("Custom user info " +  sqlContext.getConf("spark.ssb.user"));

        // For knowing where plugin is running, will remove when in production
        String hostName = "unknown";
        String hostAddress = "unknown";
        try {
            InetAddress ip = InetAddress.getLocalHost();
            hostName = ip.getHostName();
            System.out.println(hostName);
            hostAddress = ip.getHostAddress();
            System.out.println(hostAddress);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        // For testing call from spark on dataproc to service mesh
        Option<String> uriToDaplaPluginServer = parameters.get("uri_to_dapla_plugin_server");
        if (uriToDaplaPluginServer.isDefined()) {
            String[] parts = uriToDaplaPluginServer.get().split(":");
            if (parts.length < 2) {
                throw new IllegalArgumentException("option 'uri_to_dapla_plugin_server' need to have a host:port, was " + uriToDaplaPluginServer.get());
            }
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            String message = "Hello from dapla-spark-plugin! host:'" + hostName + "' ip: '" + hostAddress + "'";
            new SparkPluginClient(host, port).sayHelloToServer(message);
        }

        List<URI> dataURIs = getUriFromPath(parameters);
        return new GsimRelation(isolatedContext(sqlContext, parameters), dataURIs);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.debug("CreateRelation via write {}", parameters);
        System.out.println("CreateRelation via write - " + parameters);

        List<URI> dataURIs = getUriFromPath(parameters);
        URI newDataUri = dataURIs.get(0);

        Lock datasetLock = new ReentrantLock();
        datasetLock.lock();
        try {
            log.debug("writing file(s) to: {}", newDataUri);
            setUserContext(sqlContext.sparkSession(), "write", parameters);
            sqlContext.sparkSession().conf().set("fs.gs.impl.disable.cache", "false");
            data.coalesce(1).write().parquet(newDataUri.toASCIIString());
            return new GsimRelation(isolatedContext(sqlContext, parameters), dataURIs);
        } finally {
            datasetLock.unlock();
            sqlContext.sparkSession().conf().set("fs.gs.impl.disable.cache", "true");
        }
    }

    /**
     * Creates a new SQLContext with an isolated spark session.
     *
     * @param sqlContext the original SQLContext (which will be the parent context)
     * @param parameters parameter map with additional spark options
     * @return the new SQLContext
     */
    private SQLContext isolatedContext(SQLContext sqlContext, Map<String, String> parameters) {
        // Temporary enable file system cache during execution. This aviods re-creating the GoogleHadoopFileSystem
        // during multiple job executions within the spark session.
        // For this to work, we must create an isolated configuration inside a new spark session
        // Note: There is still only one spark context that is shared among sessions
        SparkSession sparkSession = sqlContext.sparkSession().newSession();
        sparkSession.conf().set("fs.gs.impl.disable.cache", "false");
        setUserContext(sparkSession, "read", parameters);
        return sparkSession.sqlContext();
    }

    // TODO: This should only be set when the user has access to the current operation and namespace
    private void setUserContext(SparkSession sparkSession, String operation, Map<String, String> parameters) {
        String namespace = getNamespace(parameters);
        Text service = getService(parameters);
        sparkSession.conf().set(BrokerTokenIdentifier.CURRENT_NAMESPACE, namespace);
        sparkSession.conf().set(BrokerTokenIdentifier.CURRENT_OPERATION, operation);
        try {
            UserGroupInformation.getCurrentUser().addToken(service,
                    BrokerDelegationTokenBinding.createUserToken(service, new Text(operation), new Text(namespace)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: Replace by calling catalog service
    private String getNamespace(Map<String, String> parameters) {
        return getUriFromPath(parameters).get(0).toString();
    }

    // TODO: Replace by calling catalog service
    private Text getService(Map<String, String> parameters) {
        URI path = getUriFromPath(parameters).get(0);
        return new Text(path.getScheme() + "://" + path.getAuthority());
    }

    private List<URI> getUriFromPath(Map<String, String> parameters) {
        Option<String> path = parameters.get("PATH");
        if (path.isEmpty()) {
            throw new IllegalStateException("PATH missing from parameters" + parameters);
        }
        try {
            URI uri = new URI(path.get());
            return Collections.singletonList(uri);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
