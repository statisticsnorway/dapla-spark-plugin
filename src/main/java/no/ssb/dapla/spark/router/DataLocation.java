package no.ssb.dapla.spark.router;

import java.net.URI;
import java.util.List;

public class DataLocation {
    private final String namespace;
    private final String location;
    private final List<URI> paths;

    public DataLocation(String namespace, String location, List<URI> paths) {
        this.namespace = namespace;
        this.location = location;
        this.paths = paths;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getLocation() {
        return location;
    }

    public List<URI> getPaths() {
        return paths;
    }
}
