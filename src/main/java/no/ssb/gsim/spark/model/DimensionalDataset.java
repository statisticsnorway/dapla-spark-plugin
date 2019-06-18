package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionalDataset extends Dataset {

    @JsonProperty
    private String dimensionalDataStructure;

    public String getDimensionalDataStructure() {
        return dimensionalDataStructure;
    }

    public void setDimensionalDataStructure(String dimensionalDataStructure) {
        this.dimensionalDataStructure = dimensionalDataStructure;
    }
}
