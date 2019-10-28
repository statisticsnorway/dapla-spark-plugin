package no.ssb.gsim.spark;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.*;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GsimRelation extends BaseRelation implements PrunedFilteredScan {

    private final SQLContext context;
    private final Set<String> files;
    private StructType schema;

    public GsimRelation(SQLContext context, List<URI> uris) {
        this.context = context;
        this.files = uris.stream()
                .map(URI::toASCIIString)
                .collect(Collectors.toSet());
    }

    /**
     * The equals should return true if it is known that the two relations will return the
     * same data. Using the set of files guaranties this.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GsimRelation that = (GsimRelation) o;
        return files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return files.hashCode();
    }

    @Override
    public SQLContext sqlContext() {
        return this.context;
    }

    @Override
    public synchronized StructType schema() {
        // Memoize.
        if (schema == null) {
            StructType readSchema = this.sqlContext().read()
                    .parquet(files.toArray(new String[]{})).schema();
            List<StructField> modifiedFields = new ArrayList<>();
            for (StructField field : readSchema.fields()) {
                if (field.name().equals("MUNICIPALITY")) {
                    modifiedFields.add(new StructField("MUNICIPALITY", DataTypes.StringType, true,
                            Metadata.fromJson("{\"test\": true}")));
                } else {
                    modifiedFields.add(field);
                }
            }
            schema = new StructType(modifiedFields.toArray(new StructField[]{}));
        }
        return schema;
    }

    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters) {
        Dataset<Row> dataset = this.sqlContext().read().parquet(files.toArray(new String[]{}));
        Column[] columns = Stream.of(requiredColumns).map(dataset::col).toArray(Column[]::new);
        return dataset.select(columns).rdd();
    }
}
