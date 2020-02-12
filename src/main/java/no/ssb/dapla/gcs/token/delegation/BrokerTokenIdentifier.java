package no.ssb.dapla.gcs.token.delegation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier;

/**
 * Represents a delegation token identifier (i.e. a "session token" identifier) that needs to be present before
 * the {@link no.ssb.dapla.gcs.token.broker.BrokerAccessTokenProvider} will issue access tokens.
 */
public class BrokerTokenIdentifier extends DelegationTokenIdentifier {

    public static final Text KIND = new Text("GCPBrokerSessionToken");
    private Text operation;
    private Text namespace;

    public static class Renewer extends Token.TrivialRenewer {
        public Renewer() {
        }

        protected Text getKind() {
            return BrokerTokenIdentifier.KIND;
        }
    }

    public BrokerTokenIdentifier() {
        super(KIND);
        setOperation(null);
        setNamespace(null);
    }

    public BrokerTokenIdentifier(Text owner, Text renewer, Text realUser, Text operation, Text namespace) {
        super(KIND, owner, renewer, realUser);
        setOperation(operation);
        setNamespace(namespace);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        operation.write(out);
        namespace.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        operation.readFields(in, Text.DEFAULT_MAX_LEN);
        namespace.readFields(in, Text.DEFAULT_MAX_LEN);
    }

    public Text getOperation() {
        return operation;
    }

    private void setOperation(Text operation) {
        if (operation == null) {
            this.operation = new Text();
        } else {
            this.operation = operation;
        }
    }

    public Text getNamespace() {
        return namespace;
    }

    private void setNamespace(Text namespace) {
        if (namespace == null) {
            this.namespace = new Text();
        } else {
            this.namespace = namespace;
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(super.toString()).append(", operation=" + operation + ", namespace=" + namespace);
        return buffer.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrokerTokenIdentifier)) return false;
        if (!super.equals(o)) return false;
        BrokerTokenIdentifier that = (BrokerTokenIdentifier) o;
        return Objects.equals(operation, that.operation) &&
                Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), operation, namespace);
    }

    public static class Builder {
        private Text service;
        private Text operation;
        private Text namespace;
        private Text realUser;

        public BrokerTokenIdentifier.Builder withService(Text service) {
            this.service = service;
            return this;
        }

        public BrokerTokenIdentifier.Builder withOperation(Text operation) {
            this.operation = operation;
            return this;
        }

        public BrokerTokenIdentifier.Builder withNamespace(Text namespace) {
            this.namespace = namespace;
            return this;
        }

        public BrokerTokenIdentifier.Builder withRealUser(Text realUser) {
            this.realUser = realUser;
            return this;
        }

        public BrokerTokenIdentifier build() {
            try {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                String user = ugi.getUserName();
                Text owner = new Text(user);
                if (realUser == null && ugi.getRealUser() != null) {
                    realUser = new Text(ugi.getRealUser().getUserName());
                }
                return new BrokerTokenIdentifier(owner, service, realUser, operation, namespace);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}