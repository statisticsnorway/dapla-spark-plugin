package no.ssb.dapla.spark.plugin;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import org.apache.directory.shared.kerberos.codec.apRep.actions.ApRepInit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
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
        System.out.println("CreateRelation via read - ");

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

        Option<String> restUriToDaplaPluginServer = parameters.get("rest_uri_to_dapla_plugin_server");
        Option<String> bearerToken = parameters.get("bearer_token");
        if (restUriToDaplaPluginServer.isDefined()) {
            String url = restUriToDaplaPluginServer.get();
            String token = bearerToken.isDefined() ? bearerToken.get() : "";
            simpleGet(url, "Bearer " + token);
        }

        List<URI> dataURIs = getUriFromPath(parameters);
        return new GsimRelation(sqlContext, dataURIs);
    }

    private void simpleGet(String url, String bearerToken) {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Authorization", bearerToken)
                .build();
        try {
            System.out.println("Running simple get:");
            Response response = client.newCall(request).execute();
            System.out.println(response);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
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
            return new GsimRelation(sqlContext, dataURIs);
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

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
