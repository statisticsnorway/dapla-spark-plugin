package no.ssb.dapla.spark.plugin.pseudo;

import no.ssb.dapla.spark.plugin.GsimDatasourceTest;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.collection.immutable.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class PseudoContextTest {

    private SQLContext sqlContext;
    private SparkContext sparkContext;

    @Before
    public void setUp() throws Exception {
        SparkSession session = SparkSession.builder()
          .appName(GsimDatasourceTest.class.getSimpleName())
          .master("local")
          .config("spark.ui.enabled", false)
          .getOrCreate();

        this.sparkContext = session.sparkContext();
        this.sqlContext = session.sqlContext();

    }

    @After
    public void tearDown() {
        sparkContext.stop();
    }

    @Test
    public void newContext_shouldRegisterUDFs() {
        Map<String, String> options = new Map.Map1("pseudo", "INCOME=fpe-digits-123,MUNICIPALITY=fpe-digits-123,DATA_QUALITY=fpe-alphanumeric-123");
        PseudoContext pseudoContext = new PseudoContext(sqlContext, options);
        assertThat(pseudoContext.registeredUDFs()).containsExactlyInAnyOrder(
          "pseudo_apply_boolean_udf",
          "pseudo_apply_date_udf",
          "pseudo_apply_double_udf",
          "pseudo_apply_float_udf",
          "pseudo_apply_integer_udf",
          "pseudo_apply_long_udf",
          "pseudo_apply_string_udf",
          "pseudo_apply_timestamp_udf",
          "pseudo_restore_boolean_udf",
          "pseudo_restore_date_udf",
          "pseudo_restore_double_udf",
          "pseudo_restore_float_udf",
          "pseudo_restore_integer_udf",
          "pseudo_restore_long_udf",
          "pseudo_restore_string_udf",
          "pseudo_restore_timestamp_udf"
        );
    }
}