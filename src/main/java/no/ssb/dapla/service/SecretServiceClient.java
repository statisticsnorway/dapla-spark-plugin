package no.ssb.dapla.service;

import org.apache.directory.api.util.Base64;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SecretServiceClient {

    private static final Map<String, Secret> secretRepo = new HashMap<>();

    public Secret createOrGetSecret(String secretId, String type) {
        System.out.println("createOrGetSecret " + secretId);
        Secret secret = secretRepo.get(secretId);
        if (secret == null) {
            secretRepo.put(secretId, createSecret(secretId, type));
        }

        return secretRepo.get(secretId);
    }

    public Secret getSecret(String secretId) {
        System.out.println("getSecret " + secretId);
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

    public static class SecretServiceClientException extends RuntimeException {
        public SecretServiceClientException(String message) {
            super(message);
        }

        public SecretServiceClientException(String message, Throwable cause) {
            super(message, cause);
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
}
