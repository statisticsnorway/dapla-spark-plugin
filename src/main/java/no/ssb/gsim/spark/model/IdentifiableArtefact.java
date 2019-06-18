package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import no.ssb.gsim.spark.model.api.Configured;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class IdentifiableArtefact extends Configured {

    @JsonProperty
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
