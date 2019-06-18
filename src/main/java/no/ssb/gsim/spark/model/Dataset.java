package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Example: https://lds-client.staging.ssbmod.net/be/lds-c/ns/UnitDataSet/b9c10b86-5867-4270-b56e-ee7439fe381e
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Dataset extends IdentifiableArtefact {

    @JsonProperty
    private String dataSourcePath;

    public String getDataSourcePath() {
        return dataSourcePath;
    }

    public void setDataSourcePath(String dataSourcePath) {
        this.dataSourcePath = dataSourcePath;
    }
}
