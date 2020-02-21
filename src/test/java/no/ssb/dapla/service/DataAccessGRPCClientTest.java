package no.ssb.dapla.service;

import org.junit.Ignore;
import org.junit.Test;

public class DataAccessGRPCClientTest {

    @Test
    @Ignore
    public void getTokenResponse() {
        DataAccessGRPCClient dataAccessGRPCClient = new DataAccessGRPCClient("localhost", 10148);

        dataAccessGRPCClient.getTokenResponse();

    }
}