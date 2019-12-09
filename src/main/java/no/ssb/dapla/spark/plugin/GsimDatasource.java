package no.ssb.dapla.spark.plugin;

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
    private static final String CONFIG = "spark.ssb.gsim.";
    // The default location used when writing data.
    static final String CONFIG_LOCATION_PREFIX = CONFIG + "location";
    // The lds url to use. Required.
    static final String CONFIG_LDS_URL = CONFIG + "ldsUrl";
    // oAuth parameters. Must all be set to be used.
    private final Logger log = LoggerFactory.getLogger(this.getClass());


    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        log.debug("CreateRelation via read {}", parameters);
        System.out.println("CreateRelation via read - " + parameters);
        try {
            InetAddress ip = InetAddress.getLocalHost();
            System.out.println(ip.getHostName());
            System.out.println(ip.getHostAddress());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        new SparkPluginClient().call(parameters.mkString());

        List<URI> dataURIs = getUriFromPath(parameters);
        return new GsimRelation(sqlContext, dataURIs);
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
