package no.ssb.dapla.spark.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;
import no.ssb.lds.gsim.okhttp.UnitDataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.jetbrains.annotations.NotNull;
import scala.collection.immutable.Map;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class DatasetHelper {
    static final String CREATE_DATASET = "create";
    static final String USER_NAME = "user_name";
    static final String CRATE_GSIM_OBJECTS = "create_gsim_objects";
    static final String USE_THIS_ID = "use_this_id";
    static final String DESCRIPTION = "description";
    private static final String PATH = "path";
    private final Map<String, String> parameters;
    private final String locationPrefix;
    private final SaveMode saveMode;
    private final String cachedNewDataSetId = UUID.randomUUID().toString();
    private URI cachedDatasetUri;
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

    public static List<URI> splitDataURIs(String paths) {
        return Stream.of(paths.split(",")).map(URI::create).collect(Collectors.toList());
    }

    public static String joinDataURIs(List<URI> URIs) {
        return URIs.stream().map(URI::toASCIIString).collect(Collectors.joining(","));
    }

    public static URI createNewDataURI(URI locationPrefix, String id) {
        // prepend the locationPrefix's ssp to the datasetUri's ssp
        // and add the current time in ms.
        String sspWithPrefixAndVersion = String.format("%s/%s/%d",
                locationPrefix.getSchemeSpecificPart(),
                id,
                System.currentTimeMillis()
        );
        try {
            return new URI(locationPrefix.getScheme(), sspWithPrefixAndVersion, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(String.format("could not generate versioned URI %s",
                    sspWithPrefixAndVersion), e);
        }
    }

    static String extractId(final URI uri) {
        List<String> pathParts = Arrays.asList(uri.getPath().split("/"));
        return pathParts.get(pathParts.size() - 1);
    }

    static Instant extractTimestamp(final URI uri) {
        String fragment = uri.getFragment();
        if (fragment == null) {
            return Instant.now();
        } else {
            return Instant.parse(fragment);
        }
    }

    /**
     * Normalize the URI.
     */
    static URI normalizeURI(final URI uri, final URI ldsUri) throws URISyntaxException {
        if (uri.isAbsolute()) {
            String scheme = uri.getScheme();
            if (!"lds+gsim".equals(scheme) && !"gsim+lds".equals(scheme)) {
                throw new URISyntaxException("unsupported scheme", "", 0);
            }
        }
        final String path;
        if (uri.isOpaque()) {
            path = uri.getSchemeSpecificPart();
        } else {
            if (uri.isAbsolute()) {
                path = uri.getPath();
            } else {
                path = ldsUri.getPath() + "/" + uri.getPath();
            }

        }

        final String scheme = ldsUri.getScheme();
        final String host = uri.getHost() != null ? uri.getHost() : ldsUri.getHost();
        final int port = uri.getHost() != null ? uri.getPort() : ldsUri.getPort();
        final String fragment = uri.getFragment();
        return new URI(
                scheme,
                null,
                host,
                port,
                path,
                null,
                fragment
        );
    }

    @NotNull
    static URI validatePath(Map<String, String> parameters) {
        URI pathURI;
        try {
            pathURI = new URI(parameters.get("PATH").get());
        } catch (NoSuchElementException nse) {
            pathURI = URI.create("");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("path was not an URI");
        }
        return pathURI;
    }

    @NotNull
    static URI validateLocationPrefix(SQLContext sqlContext) {
        URI locationPrefix;
        try {
            locationPrefix = new URI(sqlContext.getConf(GsimDatasource.CONFIG_LOCATION_PREFIX));
        } catch (NoSuchElementException nse) {
            throw new IllegalArgumentException("location prefix not set", nse);
        } catch (URISyntaxException use) {
            throw new IllegalArgumentException("location prefix was not an URI", use);
        }
        return locationPrefix;
    }

    public static class CreateParameters {

        @JsonProperty(value = "create", required = true)
        public String name;

        @JsonProperty(value = "description", required = true)
        public String description;

        @JsonProperty(value = "use_this_id")
        public String id;

        @JsonProperty(value = "user_name")
        public String createdBy;

        @JsonProperty(value = "create_gsim_objects")
        public Boolean createGsimObjects;

    }
}
