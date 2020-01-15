package no.ssb.dapla.service;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import no.ssb.dapla.catalog.protobuf.Dataset;
import no.ssb.dapla.utils.ProtobufJsonUtils;

import java.io.IOException;

public class SparkServiceClient {
    public Dataset getDataset() {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url("http://localhost:18060/dataset-meta?name=skatt2019.konto&operation=READ&userId=12")
//                .addHeader("Authorization", bearerToken)
                .build();
        try {
            System.out.println("Running simple get:");
            Response response = client.newCall(request).execute();
            System.out.println(response);
            String json = response.body().string();
            System.out.println(json);
            return ProtobufJsonUtils.toPojo(json, Dataset.class);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
