package no.ssb.dapla.gcs.token.delegation;

import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

// Maybe used by hadoop
public class BrokerTokenIdentifierSelector extends AbstractDelegationTokenSelector {

    public BrokerTokenIdentifierSelector() {
        super(BrokerTokenIdentifier.KIND);
    }
}
