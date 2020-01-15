package no.ssb.dapla.spark.router;


import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SaveMode;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class routes calls from spark plugin to remote services.
 */
public class SparkServiceRouter {

    private final String bucket;

    // Mock access rules from userId and namespace
    private Map<String, Set<String>> authMock = new ConcurrentHashMap<>();

    // Mock a mapping from namespace to dataset-locations
    private Map<String, DataLocation> catalogMock = new ConcurrentHashMap<>();

    public SparkServiceRouter(String bucket) {
        this.bucket = bucket;
    }

    private static SparkServiceRouter instance;

    public static SparkServiceRouter getInstance(String bucket) {
        if (instance == null) {
            instance = new SparkServiceRouter(bucket);
        }
        return instance;
    }

    public DataLocation read(String userId, String namespace) {
        System.out.println("Brukernavn: " + userId);
        if (authMock.get(userId) == null || !authMock.get(userId).contains(namespace)) {
            throw new RuntimeException(String.format("Din bruker %s har ikke tilgang til %s", userId, namespace));
        }
        DataLocation location = catalogMock.get(namespace);
        if (location != null) {
            System.out.println("Fant følgende datasett: " + StringUtils.join(location.getPaths().stream().map(URI::toString).toArray(), ", "));
        } else {
            System.out.println("Fant ingen datasett");
        }
        return location;
    }

    public DataLocation write(SaveMode mode, String userId, String namespace, String dataId) {
        try {
            System.out.println("Oppretter datasett: " + dataId);
            final DataLocation dataLocation = new DataLocation(namespace, bucket, Arrays.asList(new URI(dataId)));
            if (authMock.get(userId) == null) {
                authMock.put(userId, Collections.singleton(namespace));
            } else {
                Set<String> namespaces = new HashSet<>(authMock.get(userId));
                namespaces.add(namespace);
                authMock.put(userId, namespaces);
            }
            if (mode == SaveMode.Overwrite) {
                catalogMock.put(namespace, dataLocation);
                return dataLocation;
            } else if (mode == SaveMode.Append) {
                DataLocation dl = catalogMock.get(namespace);
                if (dl != null) {
                    dl.getPaths().add(new URI(dataId));
                } else {
                    catalogMock.put(namespace, dataLocation);
                    dl = dataLocation;
                }
                return dl;
            } else {
                return catalogMock.get(namespace);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}
