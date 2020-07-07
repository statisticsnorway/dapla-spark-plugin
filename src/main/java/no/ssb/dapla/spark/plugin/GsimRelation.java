package no.ssb.dapla.spark.plugin;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.FileRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public class GsimRelation extends BaseRelation implements PrunedFilteredScan, FileRelation, TableScan {

    private static final Logger log = LoggerFactory.getLogger(GsimRelation.class);

    private final SQLContext context;
    private final String path;
    private final StructType schema;

    public GsimRelation(SQLContext context, String path) {
        this.context = context;
        this.path = path;
        this.schema = getDataset().schema();
    }

    public GsimRelation(SQLContext context, String path, StructType schema) {
        this.context = context;
        this.path = path;
        this.schema = schema;
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
    public StructType schema() {
        return schema;
    }

    @Override
    public RDD<Row> buildScan(String[] columns, Filter[] filters) {
        Column[] requiredColumns = Stream.of(columns).map(Column::new).toArray(Column[]::new);
        boolean andFilterIsNull = Arrays.stream(filters).anyMatch(Objects::nonNull);
        Optional<Column> filter = andFilterIsNull ? Optional.empty()
                : Stream.of(filters).map(this::convertFilter).reduce(Column::and);

        Dataset<Row> dataset = getDataset();
        dataset = dataset.select(requiredColumns);
        if (filter.isPresent()) {
            dataset = dataset.filter(filter.get());
        }

        return dataset.rdd();
    }

    @Override
    public RDD<Row> buildScan() {
        return getDataset().rdd();
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
            log.warn("Could not convert {} to Column", filter);
            return null;
        }
    }

    private Dataset<Row> getDataset() {
        return this.sqlContext().read().parquet(path);
    }

    @Override
    public String[] inputFiles() {
        return new String[]{path};
    }
}
