package no.ssb.gsim.spark;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.StructType;

import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

public class GsimRelation extends BaseRelation implements PrunedFilteredScan {

    private final SQLContext context;
    private final String[] files;

    public GsimRelation(SQLContext context, List<URI> uris) {
        this.context = context;
        this.files = uris.stream().map(URI::toASCIIString).toArray(String[]::new);
    }

    @Override
    public SQLContext sqlContext() {
        return this.context;
    }

    @Override
    public StructType schema() {
        return this.sqlContext().read().parquet(files).schema();
    }

    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
        Dataset<Row> dataset = this.sqlContext().read().parquet(files);
        Column[] columns = Stream.of(requiredColumns).map(dataset::col).toArray(Column[]::new);
        return dataset.select(columns).rdd();
    }
}
