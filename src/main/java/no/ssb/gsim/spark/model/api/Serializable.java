package no.ssb.gsim.spark.model.api;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public interface Serializable<T> {

    byte[] serialize(ObjectMapper mapper, T object) throws IOException;
}
