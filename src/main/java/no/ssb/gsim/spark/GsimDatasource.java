package no.ssb.gsim.spark;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.collection.immutable.Map;

import java.net.URI;
import java.util.ArrayList;

public class GsimDatasource implements RelationProvider {

    public static final String PATH = "path";

    @Override
    public BaseRelation createRelation(final SQLContext sqlContext, Map<String, String> parameters) {

        Option<String> pathOption = parameters.get(PATH);
        if (pathOption.isEmpty()) {
            throw new RuntimeException("'path' must be set");
        }

        // Validate the path.
        URI uri = URI.create(pathOption.get());

        ArrayList<URI> actualPaths = new ArrayList<>();
        actualPaths.add(URI.create("file:///home/hadrien/Projects/SSB/java-vtl-connectors/java-vtl-connectors-parquet/8986396a-be56-434e-8491-fd1148d8c2b9.parquet"));
        actualPaths.add(URI.create("file:///home/hadrien/Projects/SSB/java-vtl-connectors/java-vtl-connectors-parquet/8986396a-be56-434e-8491-fd1148d8c2b9.parquet"));

        return new GsimRelation(sqlContext, actualPaths);
    }
}
