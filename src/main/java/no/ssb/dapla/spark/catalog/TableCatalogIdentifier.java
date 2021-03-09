package no.ssb.dapla.spark.catalog;

import com.google.common.collect.Streams;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TableCatalogIdentifier {

    private final TableIdentifier identifier;
    private static final Pattern VALID_CHARS = Pattern.compile("([^\\w]|_)+");
    private static final Pattern CODEPOINT = Pattern.compile("![0-9]{4}");
    private static final Pattern INVALID_CATALOG_CHARS = Pattern.compile("\\.");

    private TableCatalogIdentifier(TableIdentifier identifier) {
        this.identifier = identifier;
    }

    public static TableCatalogIdentifier fromTableIdentifier(TableIdentifier identifier) {
        return new TableCatalogIdentifier(identifier);
    }
    /**
     * Convert from path based identifier to catalog identifiers
     */
    public static TableCatalogIdentifier fromPath(String path) {
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        String[] escaped = escape(path.split("/")).toArray(String[]::new);
        return new TableCatalogIdentifier(TableIdentifier.of(escaped));
    }

    public String toCatalogPath(String catalogName) {
        return Joiner.on(".").join(catalogName, unescape().map(part ->
                INVALID_CATALOG_CHARS.matcher(part).find() ? "'" + part  + "'" : part
        ).collect(Collectors.joining(".")));
    }

    String toPath() {
        String path = unescape().collect(Collectors.joining("/"));
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }

    @NotNull
    private static Stream<String> escape(String... values) {
        return Stream.of(values)
                .map(part -> VALID_CHARS.matcher(part).replaceAll(match -> match.group().codePoints()
                        .mapToObj(codePoint -> String.format("!%04d", codePoint))
                        .collect(Collectors.joining())));
    }

    @NotNull
    private Stream<String> unescape() {
        return Streams.concat(Stream.of(identifier.namespace().levels()), Stream.of(identifier.name()))
                .map(part -> CODEPOINT.matcher(part).replaceAll(match -> {
                    int point = Integer.parseInt(match.group().substring(1, 5));
                    return String.valueOf(Character.toChars(point));
                }));
    }

    @Override
    public String toString() {
        return "TableCatalogIdentifier{" +
                "identifier=" + identifier +
                '}';
    }
}
