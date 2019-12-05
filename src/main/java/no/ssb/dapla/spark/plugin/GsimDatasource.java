package no.ssb.dapla.spark.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.avro.convert.gsim.LdsGsimWriter;
import no.ssb.avro.convert.gsim.SchemaToGsim;
import no.ssb.lds.gsim.okhttp.UnitDataset;
import no.ssb.lds.gsim.okhttp.api.Client;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.RelationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static scala.collection.JavaConversions.mapAsJavaMap;

public class GsimDatasource implements RelationProvider, CreatableRelationProvider, DataSourceRegister {
    private static final String SHORT_NAME = "gsim";
    private static final String CONFIG = "spark.ssb.gsim.";
    // The default location used when writing data.
    static final String CONFIG_LOCATION_PREFIX = CONFIG + "location";
    // The lds url to use. Required.
    static final String CONFIG_LDS_URL = CONFIG + "ldsUrl";
    // oAuth parameters. Must all be set to be used.
    private static final String CONFIG_LDS_OAUTH_TOKEN_URL = CONFIG + "oauth.tokenUrl";
    private static final String CONFIG_LDS_OAUTH_CLIENT_ID = CONFIG + "oauth.clientId";
    private static final String CONFIG_LDS_OAUTH_CLIENT_SECRET = CONFIG + "oauth.clientSecret";
    private static final String CONFIG_LDS_OAUTH_USER_NAME = CONFIG + "oauth.userName";
    private static final String CONFIG_LDS_OAUTH_PASSWORD = CONFIG + "oauth.password";
    private static final String CONFIG_LDS_OAUTH_GRANT_TYPE = CONFIG + "oauth.grantType";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private Client createLdsClient(final SparkConf conf) {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        createOAuth2Interceptor(conf).ifPresent(builder::addInterceptor);
        return new Client()
                .withClient(builder.build())
                .withMapper(MAPPER)
                .withPrefix(HttpUrl.parse(conf.get(CONFIG_LDS_URL)));
    }

    private Optional<OAuth2Interceptor> createOAuth2Interceptor(final SparkConf conf) {
        if (conf.contains(CONFIG_LDS_OAUTH_TOKEN_URL)) {
            OAuth2Interceptor interceptor = new OAuth2Interceptor(
                    conf.get(CONFIG_LDS_OAUTH_TOKEN_URL, null),
                    OAuth2Interceptor.GrantType.valueOf(
                            conf.get(CONFIG_LDS_OAUTH_GRANT_TYPE).toUpperCase()
                    ),
                    conf.get(CONFIG_LDS_OAUTH_CLIENT_ID, null),
                    conf.get(CONFIG_LDS_OAUTH_CLIENT_SECRET, null),
                    conf.get(CONFIG_LDS_OAUTH_USER_NAME, null),
                    conf.get(CONFIG_LDS_OAUTH_PASSWORD, null)
            );
            return Optional.of(interceptor);
        }
        return Optional.empty();
    }

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {
        log.debug("CreateRelation via read {}", parameters);

        Client ldsClient = createLdsClient(sqlContext.sparkContext().conf());
        URI pathURI = DatasetHelper.validatePath(parameters);
        URI ldsPrefixURI = ldsClient.getPrefix().uri();

        URI ldsURI = null;
        try {
            ldsURI = DatasetHelper.normalizeURI(pathURI, ldsPrefixURI);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("could not generate LDS uri", e);
        }

        UnitDataset dataset = ldsClient.fetchUnitDataset(
                DatasetHelper.extractId(ldsURI), DatasetHelper.extractTimestamp(ldsURI)
        ).join();

        List<URI> dataURIs = DatasetHelper.splitDataURIs(dataset.getDataSourcePath());
        return new GsimRelation(sqlContext, dataURIs);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        log.debug("CreateRelation via write {}", parameters);

        Client ldsClient = createLdsClient(sqlContext.sparkContext().conf());
        URI pathURI = DatasetHelper.validatePath(parameters);
        URI ldsURI = ldsClient.getPrefix().uri();

        URI ldsUri = null;
        try {
            ldsUri = DatasetHelper.normalizeURI(pathURI, ldsURI);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("could not generate LDS uri", e);
        }

        Lock datasetLock = new ReentrantLock();
        datasetLock.lock();
        try {

            final UnitDataset dataset;
            if (parameters.get("PATH").isEmpty() || !parameters.get("CRATE_GSIM_OBJECTS").isEmpty()) {

                // It the PATH is empty, we expect to find enough information in the
                // parameters to create a dataset object in the LDS. Here we use jackson
                // to create a representation (CreateParameters) of the options passed
                // from spark.
                DatasetHelper.CreateParameters createParameters = MAPPER.convertValue(mapAsJavaMap(parameters),
                        DatasetHelper.CreateParameters.class);

                SparkToParquetSchemaConverter sparkToParquetSchemaConverter = new SparkToParquetSchemaConverter(sqlContext.conf());
                MessageType messageType = sparkToParquetSchemaConverter.convert(data.schema());
                AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
                Schema schema = avroSchemaConverter.convert(messageType);

                LdsGsimWriter ldsGsimWriter = new LdsGsimWriter(ldsClient);
                SchemaToGsim schemaToGsim = new SchemaToGsim(schema, ldsGsimWriter, createParameters.createdBy,
                        createParameters.createGsimObjects);

                dataset = schemaToGsim.generateGsimInLds(
                        createParameters.id != null ? createParameters.id : UUID.randomUUID().toString(),
                        createParameters.name, createParameters.description);

            } else {

                dataset = ldsClient.fetchUnitDataset(
                        DatasetHelper.extractId(ldsUri), Instant.now()
                ).get();

                // In this modes we need to check if the dataset exists.
                List<URI> dataURIs = DatasetHelper.splitDataURIs(dataset.getDataSourcePath());
                if (!dataURIs.isEmpty() && mode == SaveMode.Ignore) {
                    return new GsimRelation(sqlContext, dataURIs);
                } else if (!dataURIs.isEmpty() && mode == SaveMode.ErrorIfExists) {
                    throw new IllegalArgumentException("dataset " + dataset.getId() + " already contains data");
                }
            }

            URI locationPrefix = DatasetHelper.validateLocationPrefix(sqlContext);
            URI newDataUri = DatasetHelper.createNewDataURI(locationPrefix, dataset.getId());

            final List<URI> dataURIs;
            if (mode == SaveMode.Append) {
                dataURIs = DatasetHelper.splitDataURIs(dataset.getDataSourcePath());
            } else {
                if (mode != SaveMode.Overwrite) {
                    log.warn("Using mode {} with creation is incorrect", mode);
                }
                dataURIs = new ArrayList<>();
            }
            dataURIs.add(newDataUri);

            dataset.setDataSourcePath(
                    dataURIs.stream().map(URI::toASCIIString).collect(Collectors.joining(","))
            );

            log.debug("writing file(s) to: {}", newDataUri);
            FileSystem hadoopFs = FileSystem.get(sqlContext.sparkContext().hadoopConfiguration());
            data.coalesce(1).write().parquet(newDataUri.toASCIIString());
            try {
                ldsClient.updateUnitDataset(dataset.getId(), dataset).get();
                return new GsimRelation(sqlContext, dataURIs);
            } catch (InterruptedException | ExecutionException e) {
                hadoopFs.delete(new Path(newDataUri), true);
                throw e;
            }

        } catch (IOException | ExecutionException e) {
            throw new RuntimeException("could not update lds", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted");
        } finally {
            datasetLock.unlock();
        }
    }

    @Override
    public String shortName() {
        return SHORT_NAME;
    }

}
