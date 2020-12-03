package no.ssb.dapla.spark.plugin.token;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class SparkConfStoreTest {

    @Test
    public void testWithClientAndSecret() {
        // Start a simple spark application.
        SparkSession session = SparkSession.builder()
                .config("spark.ssb.dapla.oauth.tokenUrl", "http://example.com/token")
                .config("spark.ssb.access", "testAccessToken")
                .config("spark.ssb.refresh", "testRefreshToken")
                .master("local")
                .getOrCreate();
        try {

            // Cannot reliably test this since it depends on the order the tests are run.
            // assertThat(SparkConfStore.INSTANCE).isNotNull();

            SparkConfStore store = SparkConfStore.get();

            assertThat(store.getAccessToken()).isEqualTo("testAccessToken");
            assertThat(store.getRefreshToken()).isEqualTo("testRefreshToken");
            assertThat(store.getTokenUrl()).isEqualTo("http://example.com/token");
            assertThat(store.getClientId()).isEmpty();
            assertThat(store.getClientSecret()).isEmpty();

        } finally {
            session.close();
        }
    }

    @Test
    public void testWithoutCredentialFile() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.ssb.dapla.oauth.tokenUrl", "http://example.com/token");
        sparkConf.set("spark.ssb.dapla.oauth.clientId", "testClientId");
        sparkConf.set("spark.ssb.dapla.oauth.clientSecret", "testClientSecret");
        sparkConf.set("spark.ssb.access", "testAccessToken");
        sparkConf.set("spark.ssb.refresh", "testRefreshToken");

        SparkConfStore store = new SparkConfStore(sparkConf);

        assertThat(store.getAccessToken()).isEqualTo("testAccessToken");
        assertThat(store.getRefreshToken()).isEqualTo("testRefreshToken");
        assertThat(store.getTokenUrl()).isEqualTo("http://example.com/token");
        assertThat(store.getClientId()).contains("testClientId");
        assertThat(store.getClientSecret()).contains("testClientSecret");
    }

    @Test
    public void testWithCredentialFile() {

        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.ssb.dapla.oauth.tokenUrl", "http://example.com/token");
        // Using file from classpath.
        sparkConf.set("spark.ssb.dapla.oauth.credentials.file",
                getClass().getResource("credentials.json").getFile());
        sparkConf.set("spark.ssb.access", "testAccessToken");
        sparkConf.set("spark.ssb.refresh", "testRefreshToken");

        SparkConfStore store = new SparkConfStore(sparkConf);

        assertThat(store.getAccessToken()).isEqualTo("testAccessToken");
        assertThat(store.getRefreshToken()).isEqualTo("testRefreshToken");
        assertThat(store.getTokenUrl()).isEqualTo("http://example.com/token");
        assertThat(store.getClientId()).contains("testClientIdFromFile");
        assertThat(store.getClientSecret()).contains("testClientSecretFromFile");
    }
}