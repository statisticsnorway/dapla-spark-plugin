package no.ssb.gsim.spark;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import scala.collection.immutable.Map;

public class GsimDatasource implements RelationProvider {
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, Map<String, String> parameters) {
        return null;
    }
}
