package no.ssb.dapla.spark.plugin;

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

    private SQLContext altContext(SQLContext sqlContext, Map<String, String> parameters) {
        SparkSession sparkSession = sqlContext.sparkSession();
        // This may be set from Spark/Pyspark extension
        Option<String> authToken = parameters.get("authToken");
        UserGroupInformation hadoopUser = getHadoopUser();
        if (authToken.isEmpty() && hadoopUser == null) {
            System.out.println("Can not find authToken or hadoop user name");
        } else if (!authToken.isEmpty()) {
            sparkSession.sqlContext().sessionState().conf().setConfString("fs.file.authToken", authToken.get());
        } else if (hadoopUser != null) {
            sparkSession.sqlContext().sessionState().conf().setConfString("fs.file.authToken", hadoopUser.getUserName());
        }
        return sparkSession.sqlContext();
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        log.debug("CreateRelation via read {}", parameters);
        System.out.println("CreateRelation via read - " + parameters);
        // Current user?
        try {
            UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
            System.out.println("Current user: " +  ugi.getUserName());
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        return new GsimRelation(altContext(sqlContext, parameters), dataURIs);
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
            data.coalesce(1).write().parquet(newDataUri.toASCIIString());
            return new GsimRelation(altContext(sqlContext, parameters), dataURIs);
        } finally {
            datasetLock.unlock();
        }
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

    private UserGroupInformation getHadoopUser() {
        try {
            return UserGroupInformation.getCurrentUser();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}