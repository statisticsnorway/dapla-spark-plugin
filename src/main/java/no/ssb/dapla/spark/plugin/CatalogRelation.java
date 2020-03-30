package no.ssb.dapla.spark.plugin;

import io.opentracing.Span;
import no.ssb.dapla.catalog.protobuf.DatasetId;
import no.ssb.dapla.catalog.protobuf.ListByPrefixRequest;
import no.ssb.dapla.catalog.protobuf.ListByPrefixResponse;
import no.ssb.dapla.service.CatalogClient;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

public class CatalogRelation extends BaseRelation implements TableScan {

    private final SQLContext context;
    private final String path;
    private final Dataset<Row> dataFrame;

    public class CatalogItem {
        private final String path;

        public CatalogItem(DatasetId datasetId) {
            this.path = datasetId.getPath();
        }

        public String getPath() {
            return path;
        }
    }

    public CatalogRelation(SQLContext context, String path, Span span) {
        this.context = context;
        this.path = path;
        CatalogClient catalogClient = new CatalogClient(context.sparkContext().getConf(), span);
        ListByPrefixResponse response = catalogClient.listByPrefix(ListByPrefixRequest.newBuilder().setPrefix(path).build());

        List<CatalogItem> catalogItems = response.getEntriesList().stream().map(CatalogItem::new).collect(Collectors.toList());
        this.dataFrame = sqlContext().createDataFrame(catalogItems, CatalogItem.class);
    }

    @Override
    public RDD<Row> buildScan() {
        return dataFrame.rdd();
    }

    @Override
    public StructType schema() {
        return dataFrame.schema();
    }

    /**
     * Two relations will return the same data if the path is the same.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CatalogRelation that = (CatalogRelation) o;
        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public SQLContext sqlContext() {
        return this.context;
    }

}
