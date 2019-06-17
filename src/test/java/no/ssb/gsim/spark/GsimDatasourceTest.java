package no.ssb.gsim.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class GsimDatasourceTest {

    private SparkConf config;
    private SQLContext sqlContext;
    private SparkContext sparkContext;

    @Before
    public void setUp() throws Exception {
        this.config = new SparkConf()
                .setAppName("testing provider")
                .setMaster("local");
        this.sparkContext = new SparkContext(config);
        this.sqlContext = new SQLContext(sparkContext);
    }

    @Test
    public void testRead() {
        Dataset<Row> dataset = sqlContext.read()
                .format("no.ssb.gsim.spark")
                .load("lds+gsim://lds-postgres.staging.ssbmod.net/b9c10b86-5867-4270-b56e-ee7439fe381e");
        dataset.show();
    }
}