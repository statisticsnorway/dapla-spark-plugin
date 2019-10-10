package no.ssb.gsim.spark;

import no.ssb.lds.gsim.okhttp.UnitDataset;
import org.jetbrains.annotations.NotNull;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static scala.Predef.conforms;

class DataSetHelper {
    private static final String PATH = "path";
    private static final String CREATE_DATASET = "create";
    private static final String DATASET_ID = "datasetId";

    private final Map<String, String> parameters;
    private Map<String, String> modifiedParameters;

    private UnitDataset dataset;

    DataSetHelper(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    UnitDataset getDataset() {
        return dataset;
    }

    Map<String, String> getParameters() {
        if (modifiedParameters != null) {
            return modifiedParameters;
        }
        return parameters;
    }

    boolean upDateExistingDataset() {
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

    URI extractPath() {
        Option<String> dataSetId = parameters.get("dataSetId");
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
        System.out.println("updating existing dataset" + dataset.getId());
        this.dataset = dataset;
    }

    void createdUnitDataSet(UnitDataset dataset) {
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
     * @param locationPrefix Example: hdfs://hadoop:9000
     * @return
     */
    @NotNull
    URI getDataSetUri(String locationPrefix) {
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

    private static Map<String, String> toScalaMap(java.util.Map<String, String> map) {
        return JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(
                conforms()
        );
    }

}
