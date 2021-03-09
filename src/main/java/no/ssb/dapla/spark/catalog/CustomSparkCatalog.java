package no.ssb.dapla.spark.catalog;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SSB implementation of Iceberg Catalog.
 *
 * <p>
 * A note on namespaces: SSB namespaces are implicit and do not need to be explicitly created or deleted.
 * The create and delete namespace methods are no-ops for the CustomSparkCatalog. One can still list namespaces that have
 * objects stored in them to assist with namespace-centric catalog exploration.
 * </p>
 */
public class CustomSparkCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {

    public static final String DEFAULT_CATALOG_NAME = "ssb_catalog";

    // Hadoop config
    private Configuration config;
    private final SparkConf conf;

    private static final Joiner SLASH = Joiner.on("/");

    public CustomSparkCatalog(SparkConf conf) {
        this.conf = conf;
    }

    public CustomSparkCatalog() {
        this(SparkSession.active().sparkContext().getConf());
    }

    @Override
    public void setConf(Configuration conf) {
        this.config = conf;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        // instantiate the CustomTableOperations
        return new CustomTableOperations(this.conf, this.config, tableIdentifier);
    }

    @Override
    public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
        return super.createTable(identifier, schema, spec, location, properties);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        // Location is used for both metadata-file and data-file
        return TableCatalogIdentifier.fromTableIdentifier(tableIdentifier).toPath();
    }

    @Override
    public String name() {
        return "ssb-catalog";
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        return null;
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean b) {
        throw new UnsupportedOperationException("Drop table is not supported");
    }

    @Override
    public void renameTable(TableIdentifier tableIdentifier, TableIdentifier tableIdentifier1) {
        throw new UnsupportedOperationException("Rename table is not supported");
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> map) {
        // SSB namespaces are implicit and do not need to be explicitly created or deleted.
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        String nsPath = namespace.isEmpty() ? "/" : SLASH.join(namespace.levels());
        try {
            // TODO: Call catalog listByPrefix
            return null;
        } catch (Exception ioe) {
            throw new RuntimeException(String.format("Failed to list namespace under: %s", namespace), ioe);
        }
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        // Load metadata properties for a namespace.
        String nsPath = SLASH.join(namespace.levels());
        if (!namespace.isEmpty()) {
            // TODO: Lookup catalog
            return ImmutableMap.of("location", nsPath);
        } else {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", new Object[]{namespace});
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        // SSB namespaces are implicit and do not need to be explicitly created or deleted.
        return true;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> map) throws NoSuchNamespaceException {
        // Apply a set of metadata to a namespace in the catalog.
        return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> set) throws NoSuchNamespaceException {
        // Remove a set of metadata from a namespace in the catalog.
        return false;
    }
}
