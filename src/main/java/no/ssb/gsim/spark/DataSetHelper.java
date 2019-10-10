package no.ssb.gsim.spark;

import no.ssb.lds.gsim.okhttp.UnitDataset;
import org.apache.spark.sql.SaveMode;
import org.jetbrains.annotations.NotNull;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

import static scala.Predef.conforms;

class DataSetHelper {
    private static final String PATH = "path";
    private static final String CREATE_DATASET = "create";
    private static final String DATASET_ID = "datasetId";

    private final Map<String, String> parameters;
    private final String locationPrefix;
    private final SaveMode saveMode;
    private Map<String, String> modifiedParameters;

    private UnitDataset dataset;

    /**
     * @param parameters     option from spark.write.format("no.ssb.gsim.spark).option("key", "value")
     * @param locationPrefix Example: hdfs://hadoop:9000
     */
    DataSetHelper(Map<String, String> parameters, String locationPrefix, SaveMode saveMode) {
        this.parameters = parameters;
        this.locationPrefix = locationPrefix;
        this.saveMode = saveMode;
    }

    DataSetHelper(Map<String, String> parameters, String locationPrefix) {
        this(parameters, locationPrefix, null);
    }

    UnitDataset getDataset() {
        if(saveMode != null) {
            dataset.setDataSourcePath(getDataSourcePath());
        }
        return dataset;
    }

    Map<String, String> getParameters() {
        if (modifiedParameters != null) {
            return modifiedParameters;
        }
        return parameters;
    }

    boolean updateExistingDataset() {
        Option<String> createNewDataset = parameters.get(CREATE_DATASET);
        return createNewDataset.isEmpty();
    }

    String getNewDatasetName() {
        Option<String> createNewDataset = parameters.get(CREATE_DATASET);
        if (createNewDataset.isEmpty()) {
            throw new IllegalArgumentException(CREATE_DATASET + " missing from spark option on write");
        }

        return createNewDataset.get();
    }

    private URI extractPath() {
        Option<String> dataSetId = parameters.get(DATASET_ID);
        if (dataSetId.isDefined()) {
            return URI.create("gsim+lds://" + dataSetId.get());
        }

        Option<String> pathOption = parameters.get(PATH);
        if (pathOption.isEmpty()) {
            throw new RuntimeException("'path' must be set");
        }
        return URI.create(pathOption.get());
    }

    String getDatasetId() {
        if (dataset != null) {
            return dataset.getId();
        }
        return extractDatasetId(extractPath());
    }

    void setExistingUnitDataSet(UnitDataset dataset) {
        this.dataset = dataset;
    }

    void createdUnitDataSet(UnitDataset dataset) {
//        dataset.setDataSourcePath(getDataSourcePath());
        this.dataset = dataset;

        java.util.Map<String, String> parametersAsJavaMap = JavaConverters.mapAsJavaMapConverter(parameters).asJava();
        java.util.HashMap<String, String> stringStringHashMap = new HashMap<>(parametersAsJavaMap);
        stringStringHashMap.put(DATASET_ID, dataset.getId());

        modifiedParameters = toScalaMap(stringStringHashMap);
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
     * @return URI
     */
    @NotNull
    URI getDataSetUri() {
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

    String getDataSourcePath() {
        List<URI> newDataUris = getUris(saveMode, extractUris(), getDataSetUri());
        return newDataUris.stream().map(URI::toASCIIString).collect(Collectors.joining(","));
    }

    @NotNull
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

    private static Map<String, String> toScalaMap(java.util.Map<String, String> map) {
        return JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(
                conforms()
        );
    }

}
