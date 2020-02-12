package no.ssb.dapla.service;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import no.ssb.dapla.catalog.protobuf.PseudoConfig;
import org.apache.directory.api.util.Base64;
import org.checkerframework.checker.nullness.qual.Nullable;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SecretServiceClient {

    private static final String DEFAULT_SECRET_TYPE = "AES256";
    private static final Map<String, Secret> secretRepo = new HashMap<>();
    private static Map<String, CatalogItem> catalog = Maps.uniqueIndex(ImmutableList.of(
      new CatalogItem("/path/to/some/dataset")
    ), item -> item.fqdsName);

    /** Look up a set of secrets based */

    public Set<Secret> getSecrets(String fqdsName) {
        return getSecretIdsForDataset(fqdsName).stream()
          .map(secretId -> getSecret(secretId))
          .collect(Collectors.toSet());
    }

    public Set<Secret> createOrGetDefaultSecrets(String datasetPath, Set<String> secretIds) {
        return createOrGetSecrets(datasetPath, secretIds.stream()
          .map(secretId -> new SecretDesc(secretId, DEFAULT_SECRET_TYPE))
          .collect(Collectors.toSet()));
    }

    public Set<Secret> createOrGetSecrets(String datasetPath, Set<SecretDesc> secretDescs) {
        return secretDescs.stream()
          .map(s -> createOrGetSecret(s.getId(), s.getType()))
          .collect(Collectors.toSet());
    }

    public Secret createOrGetSecret(String secretId, String type) {
        Secret secret = secretRepo.get(secretId);
        if (secret == null) {
            secretRepo.put(secretId, createSecret(secretId, type));
        }

        return secretRepo.get(secretId);
    }

    public Secret getSecret(String secretId) {
        return Optional.ofNullable(secretRepo.get(secretId)).orElseThrow(
          () -> new SecretServiceClientException("Unable to find secret with id=" + secretId)
        );
    }

    private Secret createSecret(String secretId, String type) {
        int keyLength = 32;
        try {
            keyLength = Integer.parseInt(type.replace("AES", ""));
        }
        catch (Exception e) { /* swallow */ }

        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(keyLength);
            SecretKey secretKey = keyGen.generateKey();
            return new Secret(secretId, secretKey.getEncoded(), type);
        }
        catch (Exception e) {
            throw new SecretServiceClientException("Unable to create secret of type " + type, e);
        }
    }

    Set<String> getSecretIdsForDataset(String fqdsName) {
        CatalogItem catalogItem = Optional.ofNullable(catalog.get(fqdsName)).orElseThrow(() ->
          new SecretServiceClientException("No catalog entry associated with dataset " + fqdsName)
        );

        PseudoConfig pseudoConfig = Optional.ofNullable(catalogItem.pseudoConfig).orElse(null);

        if (pseudoConfig == null) {
            return Collections.emptySet();
        }
        else {
            return pseudoConfig.getSecretsList().stream()
              .map(secretItem -> secretItem.getId())
              .collect(Collectors.toSet());
        }
    }

    public static class SecretServiceClientException extends RuntimeException {
        public SecretServiceClientException(String message) {
            super(message);
        }

        public SecretServiceClientException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class SecretDesc {
        private final String id;
        private final String type;

        public SecretDesc(String id, String type) {
            this.id = id;
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return "SecretDesc{" +
              "id='" + id + '\'' +
              ", type='" + type + '\'' +
              '}';
        }
    }

    public static class Secret {
        private final String id;
        private final byte[] body;
        private final String type;

        public Secret(String id, byte[] body, String type) {
            this.id = id;
            this.body = body;
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public byte[] getBody() {
            return body;
        }

        public String getBase64EncodedBody() {
            return String.valueOf(Base64.encode(body));
        }

        public String getType() {
            return type;
        }

        @Override
        public String toString() {
            return "Secret{" +
              "id='" + id + '\'' +
              ", base64EncodedBody='" + getBase64EncodedBody() + '\'' +
              ", type='" + type + '\'' +
              '}';
        }
    }

    private static class CatalogItem {
        private final String fqdsName;
        private PseudoConfig pseudoConfig;

        public CatalogItem(String fqdsName) {
            this.fqdsName = fqdsName;
        }


    }
}
