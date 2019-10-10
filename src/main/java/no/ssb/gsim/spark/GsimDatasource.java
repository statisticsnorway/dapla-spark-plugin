package no.ssb.gsim.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.avro.convert.gsim.LdsClient;
import no.ssb.avro.convert.gsim.SchemaToGsim;
import no.ssb.lds.gsim.okhttp.UnitDataset;
import no.ssb.lds.gsim.okhttp.api.Client;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.avro.Schema;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.datasources.parquet.SparkToParquetSchemaConverter;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;
import org.apache.parquet.avro.AvroSchemaConverter;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class GsimDatasource implements RelationProvider, CreatableRelationProvider {

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
        System.out.println("createRelation:" + parameters);

        DataSetHelper dataSetHelper = new DataSetHelper(parameters, sqlContext.getConf(CONFIG_LOCATION_PREFIX));

        String datasetId = dataSetHelper.getDatasetId();
        Client ldsClient = createLdsClient(sqlContext.sparkContext().conf());
        // TODO: send object in params if we have it
        UnitDataset dataset = ldsClient.fetchUnitDataset(datasetId, Instant.now()).join();
        dataSetHelper.setExistingUnitDataSet(dataset);
        List<URI> dataUris = dataSetHelper.extractUris();
        return new GsimRelation(sqlContext, dataUris);
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters, Dataset<Row> data) {
        Client ldsClient = createLdsClient(sqlContext.sparkContext().conf());

        DataSetHelper dataSetHelper = new DataSetHelper(parameters, sqlContext.getConf(CONFIG_LOCATION_PREFIX), mode);

        if (dataSetHelper.updateExistingDataset()) {
            // update existing UnitDataSet in lds
            UnitDataset dataset = ldsClient.fetchUnitDataset(dataSetHelper.getDatasetId(), Instant.now()).join();
            dataSetHelper.setExistingUnitDataSet(dataset);
        } else {
            // create new UnitDataSet in lds
            Schema schema = getSchema(sqlContext, data.schema());
            UnitDataset dataset = createGsimObjectInLds(schema, ldsClient, dataSetHelper.getNewDatasetName());
            dataSetHelper.createdUnitDataSet(dataset);
            System.out.println("Dataset id created:" + dataSetHelper.getDataset().getId());
        }

        URI newDataUri = dataSetHelper.getDataSetUri();
        data.coalesce(1).write().parquet(newDataUri.toASCIIString());

        System.out.println("Saving context:\n" + parameters);

        try {
            System.out.println("DataSourcePath: " + dataSetHelper.getDataset().getDataSourcePath());
            ldsClient.updateUnitDataset(dataSetHelper.getDatasetId(), dataSetHelper.getDataset()).join();

            return createRelation(sqlContext, dataSetHelper.getParameters());
        } catch (IOException e) {
            throw new RuntimeException("could not update lds", e);
        }
    }

    private Schema getSchema(SQLContext sqlContext, StructType structType) {
        SparkToParquetSchemaConverter sparkToParquetSchemaConverter = new SparkToParquetSchemaConverter(sqlContext.conf());
        MessageType messageType = sparkToParquetSchemaConverter.convert(structType);
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();

        return avroSchemaConverter.convert(messageType);
    }

    private UnitDataset createGsimObjectInLds(Schema schema, Client client, String dataSetName) {
        LdsClient ldsClient = new LdsClient(client);
        SchemaToGsim schemaToGsim = new SchemaToGsim(schema, ldsClient);

        return schemaToGsim.generateGsimInLds(dataSetName);
    }

    public static class Configuration {
        private String location;

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }
}
