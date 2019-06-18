package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionalDataStructure {

    @JsonProperty
    private List<String> instanceVariables;

    public List<String> getInstanceVariables() {
        return instanceVariables;
    }

    public void setInstanceVariables(List<String> instanceVariables) {
        this.instanceVariables = instanceVariables;
    }
}
