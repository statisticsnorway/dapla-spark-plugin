package no.ssb.gsim.spark;

import no.ssb.lds.gsim.okhttp.UnitDataset;
import org.apache.spark.sql.SaveMode;
import scala.Option;
import scala.collection.immutable.Map;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

class DatasetHelper {
    private static final String PATH = "path";

    static final String CREATE_DATASET = "create";
    static final String USER_NAME = "user_name";
    static final String CRATE_GSIM_OBJECTS = "crate_gsim_objects";
    static final String USE_THIS_ID = "user_this_id";
    static final String DESCRIPTION = "description";

    private final Map<String, String> parameters;
    private final String locationPrefix;
    private final SaveMode saveMode;
    private URI cachedDatasetUri;
    private final String cachedNewDataSetId = UUID.randomUUID().toString();

    private UnitDataset dataset;

    /**
     * @param parameters     option from spark.write.format("no.ssb.gsim.spark).option("key", "value")
     * @param locationPrefix Example: hdfs://hadoop:9000
     */
    DatasetHelper(Map<String, String> parameters, String locationPrefix, SaveMode saveMode) {
        this.parameters = parameters;
        this.locationPrefix = locationPrefix;
        this.saveMode = saveMode;
    }

    /**
     * Used for loading dataset
     *
     * @param parameters     option from spark.read.format("no.ssb.gsim.spark).option("key", "value")
     * @param locationPrefix Example: hdfs://hadoop:9000
     */
    DatasetHelper(Map<String, String> parameters, String locationPrefix) {
        this(parameters, locationPrefix, null);
    }

    UnitDataset getDataset() {
        return dataset;
    }

    boolean updateExistingDataset() {
        Option<String> createNewDataset = parameters.get(CREATE_DATASET);
        return createNewDataset.isEmpty();
    }

    boolean createGsimObjects() {
        Option<String> option = parameters.get(CRATE_GSIM_OBJECTS);
        if (option.isEmpty()) {
            return false;
        }
        try {
            return Boolean.parseBoolean(option.get());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return false;
    }

    String getUserName() {
        Option<String> option = parameters.get(USER_NAME);
        if (option.isDefined()) {
            return option.get();
        }
        return null;
    }

    String userThisId() {
        Option<String> option = parameters.get(USE_THIS_ID);
        if (option.isDefined()) {
            return option.get();
        }
        return null;
    }

    String getDescription() {
        Option<String> option = parameters.get(DESCRIPTION);
        if (option.isDefined()) {
            return option.get();
        }
        return null;
    }

    String getNewDatasetName() {
        Option<String> createNewDataset = parameters.get(CREATE_DATASET);
        if (createNewDataset.isEmpty()) {
            throw new IllegalArgumentException(CREATE_DATASET + " missing from spark option on write");
        }

        return createNewDataset.get();
    }

    /**
     * Get path to dataset
     *
     * @return Example lds+gsim://3d062358-08c2-4287-a336-a62d25c72fb9
     */
    URI extractPath() {
        if (updateExistingDataset()) {
            // extract path from save when we update/load existing dataset
            // example: spark.read.format("no.ssb.gsim.spark").save("lds+gsim://3d062358-08c2-4287-a336-a62d25c72fb9")
            Option<String> pathOption = parameters.get(PATH);
            if (pathOption.isEmpty()) {
                throw new RuntimeException("'path' must be set");
            }
            return URI.create(pathOption.get());
        }

        return URI.create("lds+gsim://" + dataset.getId());
    }

    String getDatasetId() {
        if (dataset != null) {
            return dataset.getId();
        }
        return extractDatasetId(extractPath());
    }

    void setDataSet(UnitDataset dataset) {
        this.dataset = dataset;
        if (saveMode != null) {
            dataset.setDataSourcePath(getDataSourcePath());
        }
    }

    private String extractDatasetId(URI pathUri) {
        List<String> schemes = Arrays.asList(pathUri.getScheme().split("\\+"));
        if (!schemes.contains("lds") || !schemes.contains("gsim")) {
            throw new IllegalArgumentException("invalid scheme. Please use lds+gsim://[port[:post]]/path");
        }

        String path = pathUri.getPath();
        if (path == null || path.isEmpty()) {
            // No path. Use the host as id.
            path = pathUri.getHost();
        }
        return path;
    }

    List<URI> extractUris() {
        // Find all the files for the dataset.
        List<String> dataSources = Arrays.asList(dataset.getDataSourcePath().split(","));
        return dataSources.stream().map(URI::create).collect(Collectors.toList());
    }

    /**
     * Create a new path with the current time.
     *
     * @return URI. Examples: hdfs://hadoop:9000/datasets////3d062358-08c2-4287-a336-a62d25c72fb9/1570712831638
     */
    URI getDataSetUri() {
        if (cachedDatasetUri != null) {
            return cachedDatasetUri;
        }
        cachedDatasetUri = crateDataSetUri();
        return cachedDatasetUri;
    }

    private URI crateDataSetUri() {
        URI datasetUri = extractPath();

        try {
            URI prefixUri = URI.create(locationPrefix);
            return new URI(
                    prefixUri.getScheme(),
                    String.format("%s/%s/%d",
                            prefixUri.getSchemeSpecificPart(),
                            datasetUri.getSchemeSpecificPart(), System.currentTimeMillis()
                    ),
                    null
            );
        } catch (URISyntaxException e) {
            throw new RuntimeException("could not generate new file uri", e);

        }
    }

    private String getDataSourcePath() {
        List<URI> newDataUris = getUris(saveMode, extractUris(), getDataSetUri());
        return newDataUris.stream().map(URI::toASCIIString).collect(Collectors.joining(","));
    }

    private List<URI> getUris(SaveMode mode, List<URI> dataUris, URI newDataUri) {
        List<URI> newDataUris;
        if (mode.equals(SaveMode.Overwrite)) {
            newDataUris = Collections.singletonList(newDataUri);
        } else if (mode.equals(SaveMode.Append)) {
            newDataUris = new ArrayList<>();
            newDataUris.add(newDataUri);
            newDataUris.addAll(dataUris);
        } else {
            throw new IllegalArgumentException("Unsupported mode " + mode);
        }
        return newDataUris;
    }

    String getNewDatasetId() {
        String id = userThisId();
        if (id != null) {
            return id;
        }

        return cachedNewDataSetId;
    }
}
