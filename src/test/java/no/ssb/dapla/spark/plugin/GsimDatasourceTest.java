package no.ssb.dapla.spark.plugin;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class GsimDatasourceTest {

    private static final String UNIT_DATASET_ID = "b9c10b86-5867-4270-b56e-ee7439fe381e";
    private SQLContext sqlContext;
    private SparkContext sparkContext;
    private File tempDirectory;
    private Path parquetFile;

    @After
    public void tearDown() {
        sparkContext.stop();
    }

    @Before
    public void setUp() throws Exception {
        // Create temporary folder and copy test data into it.
        tempDirectory = Files.createTempDirectory("lds-gsim-spark").toFile();
        InputStream parquetContent = this.getClass().getResourceAsStream("data/dataset.parquet");
        parquetFile = tempDirectory.toPath().resolve("dataset.parquet");
        Files.copy(parquetContent, parquetFile);

        // Read the unit dataset json example.
        SparkSession session = SparkSession.builder()
                .appName(GsimDatasourceTest.class.getSimpleName())
                .master("local")
                .config("spark.ui.enabled", false)
                .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sqlContext = session.sqlContext();

    }

    @Test
    public void testReadWithId() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.dapla.spark.plugin")
                .load(parquetFile.toString());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();
    }

    @Test
    public void testReadWrite() {
        Dataset<Row> dataset = sqlContext.read()
                .format("gsim")
                .load(parquetFile.toString());

        assertThat(dataset).isNotNull();
        assertThat(dataset.isEmpty()).isFalse();

        System.out.println(tempDirectory);
        dataset.write().format("no.ssb.dapla.spark.plugin").mode(SaveMode.Overwrite).save(tempDirectory + "/out.parquet");
    }

    @Test
    public void testSupportedUris() {

        String prefix = "http://lds:123/prefix";
        Map<String, String> uriStrings = new LinkedHashMap<>();

        // Non absolute
        uriStrings.put("datasetId", "http://lds:123/prefix/datasetId");
        uriStrings.put("datasetId#2000-01-01T00:00:00+00:00", "http://lds:123/prefix/datasetId#2000-01-01T00:00:00+00:00");
        uriStrings.put("datasetId#2000-01-01T00:00:00Z", "http://lds:123/prefix/datasetId#2000-01-01T00:00:00Z");

        // Non absolute
        uriStrings.put("ns/datasetId", "http://lds:123/prefix/ns/datasetId");
        uriStrings.put("ns/datasetId#2000-01-01T00:00:00+00:00", "http://lds:123/prefix/ns/datasetId#2000-01-01T00:00:00+00:00");
        uriStrings.put("ns/datasetId#2000-01-01T00:00:00Z", "http://lds:123/prefix/ns/datasetId#2000-01-01T00:00:00Z");

        // Opaque.
        uriStrings.put("lds+gsim:datasetId", "http://lds:123/prefix/datasetId");
        uriStrings.put("lds+gsim:datasetId#2000-01-01T00:00:00+00:00", "http://lds:123/prefix/datasetId#2000-01-01T00:00:00+00:00");
        uriStrings.put("lds+gsim:datasetId#2000-01-01T00:00:00Z", "http://lds:123/prefix/datasetId#2000-01-01T00:00:00Z");

        uriStrings.put("lds+gsim:///datasetId", "http://lds:123/datasetId");
        uriStrings.put("lds+gsim:///datasetId#2000-01-01T00:00:00+00:00", "http://lds:123/datasetId#2000-01-01T00:00:00+00:00");
        uriStrings.put("lds+gsim:///datasetId#2000-01-01T00:00:00Z", "http://lds:123/datasetId#2000-01-01T00:00:00Z");

        uriStrings.put("lds+gsim://host/datasetId", "http://host/datasetId");
        uriStrings.put("lds+gsim://host/datasetId#2000-01-01T00:00:00+00:00", "http://host/datasetId#2000-01-01T00:00:00+00:00");
        uriStrings.put("lds+gsim://host/datasetId#2000-01-01T00:00:00Z", "http://host/datasetId#2000-01-01T00:00:00Z");

        uriStrings.put("lds+gsim://host:321/datasetId", "http://host:321/datasetId");
        uriStrings.put("lds+gsim://host:321/datasetId#2000-01-01T00:00:00+00:00", "http://host:321/datasetId#2000-01-01T00:00:00+00:00");
        uriStrings.put("lds+gsim://host:321/datasetId#2000-01-01T00:00:00Z", "http://host:321/datasetId#2000-01-01T00:00:00Z");

        uriStrings.put("lds+gsim://host:321/namespace/datasetId", "http://host:321/namespace/datasetId");
        uriStrings.put("lds+gsim://host:321/namespace/datasetId#2000-01-01T00:00:00+00:00", "http://host:321/namespace/datasetId#2000-01-01T00:00:00+00:00");
        uriStrings.put("lds+gsim://host:321/namespace/../../datasetId#2000-01-01T00:00:00Z", "http://host:321/namespace/../../datasetId#2000-01-01T00:00:00Z");

        for (Map.Entry<String, String> uriEntry : uriStrings.entrySet()) {
            URI uri = URI.create(uriEntry.getKey());
            URI expectedUri = URI.create(uriEntry.getValue());
            try {
                URI result = DatasetHelper.normalizeURI(uri, URI.create(prefix));
                assertThat(result).isEqualTo(expectedUri);
            } catch (URISyntaxException e) {
                System.out.println("Failed: " + e.getMessage());
            }
        }
    }
}