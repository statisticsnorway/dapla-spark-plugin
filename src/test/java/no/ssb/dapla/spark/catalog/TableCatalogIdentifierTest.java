package no.ssb.dapla.spark.catalog;

import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

public class TableCatalogIdentifierTest {

    @Test
    public void testDaplaPath() {
        List<String> tests = List.of(
                "/some weird/p@th/to_escape",
                "/1234%&4321/_4321",
                "/path/john.doe/home",
                "/!path",
                "/with#hash",
                "/!!path"
        );
        for (String test : tests) {
            assertThat(TableCatalogIdentifier.fromPath(test).toPath()).isEqualTo(test);
        }
    }
    @Test
    public void testCatalogPath() {
        assertThat(TableCatalogIdentifier.fromTableIdentifier(TableIdentifier.of("my", "table")).toCatalogPath("catalog"))
                .isEqualTo("catalog.my.table");
        assertThat(TableCatalogIdentifier.fromPath("/user/john.doe/table").toCatalogPath("catalog"))
                .isEqualTo("catalog.user.'john.doe'.table");
    }

}
