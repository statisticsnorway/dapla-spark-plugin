package no.ssb.dapla.spark.plugin;

import no.ssb.dapla.spark.plugin.pseudo.PseudoContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.FileRelation;
import org.apache.spark.sql.sources.*;
import org.apache.spark.sql.types.*;

import java.util.*;
import java.util.stream.Stream;

public class GsimRelation extends BaseRelation implements PrunedFilteredScan, FileRelation, TableScan {

    private final SQLContext context;
    private final String path;
    private final PseudoContext pseudoContext;
    private StructType schema;

    public GsimRelation(SQLContext context, String path, PseudoContext pseudoContext) {
        this.context = context;
        this.pseudoContext = pseudoContext;
        this.path = path;
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

    @Override
    public synchronized StructType schema() {
        if (schema == null) { // Memoize.
            schema = this.sqlContext().read().load(path).schema();
        }
        return schema;
    }

    @Override
    public RDD<Row> buildScan(String[] columns, Filter[] filters) {
        Column[] requiredColumns = Stream.of(columns).map(Column::new).toArray(Column[]::new);
        Optional<Column> filter = Stream.of(filters).map(this::convertFilter).reduce(Column::and);

        Dataset<Row> dataset = this.sqlContext().read().load(path);
        dataset = dataset.select(requiredColumns);
        if (filter.isPresent()) {
            dataset = dataset.filter(filter.get());
        }
        if (pseudoContext != null) {
            dataset = pseudoContext.restore(dataset);
        }

        return dataset.rdd();
    }

    @Override
    public RDD<Row> buildScan() {
        return this.sqlContext().read().load(path).rdd();
    }

    /**
     * Converts back filters to column expression.
     * <p>
     * I could not find any function in spark to do this. This will be thrown away when
     * we migrate to DataSourceV2.
     * <p>
     * Note that the filters we receive are canonical. Thus we do not handle and/or/not.
     */
    Column convertFilter(Filter filter) {
        if (filter instanceof EqualNullSafe) {
            EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
            return new Column(equalNullSafe.attribute()).eqNullSafe(equalNullSafe.value());
        } else if (filter instanceof EqualTo) {
            EqualTo equalTo = (EqualTo) filter;
            return new Column(equalTo.attribute()).equalTo(equalTo.value());
        } else if (filter instanceof GreaterThan) {
            GreaterThan greaterThan = (GreaterThan) filter;
            return new Column(greaterThan.attribute()).gt(greaterThan.value());
        } else if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
            return new Column(greaterThanOrEqual.attribute()).geq(greaterThanOrEqual.value());
        } else if (filter instanceof In) {
            In in = (In) filter;
            return new Column(in.attribute()).isin(in.values());
        } else if (filter instanceof IsNotNull) {
            return new Column(((IsNotNull) filter).attribute()).isNotNull();
        } else if (filter instanceof IsNull) {
            return new Column(((IsNull) filter).attribute()).isNull();
        } else if (filter instanceof LessThan) {
            LessThan lessThan = (LessThan) filter;
            return new Column(lessThan.attribute()).lt(lessThan.value());
        } else if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
            return new Column(lessThanOrEqual.attribute()).leq(lessThanOrEqual.value());
        } else if (filter instanceof StringContains) {
            StringContains stringContains = (StringContains) filter;
            return new Column(stringContains.attribute()).contains(stringContains.value());
        } else if (filter instanceof StringEndsWith) {
            StringEndsWith stringEndsWith = (StringEndsWith) filter;
            return new Column(stringEndsWith.attribute()).endsWith(stringEndsWith.value());
        } else if (filter instanceof StringStartsWith) {
            StringStartsWith stringStartsWith = (StringStartsWith) filter;
            return new Column(stringStartsWith.attribute()).startsWith(stringStartsWith.value());
        } else {
            throw new UnsupportedOperationException("Could not convert " + filter + " to Column");
        }
    }

    @Override
    public String[] inputFiles() {
        return new String[]{path};
    }
}
