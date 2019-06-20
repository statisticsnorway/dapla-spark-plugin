package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.*;
import no.ssb.gsim.spark.model.api.Configured;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class IdentifiableArtefact extends Configured {

    @JsonProperty
    private String id;
    @JsonIgnore
    private Map<String, Object> unknowProperties = new LinkedHashMap<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonAnyGetter
    public Map<String, Object> getUnknowProperties() {
        return unknowProperties;
    }

    @JsonAnySetter
    public void setUnknowProperty(String key, Object value) {
        unknowProperties.put(key, value);
    }
}
