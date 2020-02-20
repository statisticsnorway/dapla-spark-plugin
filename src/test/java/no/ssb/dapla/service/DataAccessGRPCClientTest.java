package no.ssb.dapla.service;

import org.junit.Test;

public class DataAccessGRPCClientTest {

    @Test
    public void getTokenResponse() {
        DataAccessGRPCClient dataAccessGRPCClient = new DataAccessGRPCClient("localhost", 10148);

        dataAccessGRPCClient.getTokenResponse();

    }
}