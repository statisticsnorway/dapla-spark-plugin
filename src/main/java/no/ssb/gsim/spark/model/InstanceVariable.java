package no.ssb.gsim.spark.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.gsim.spark.model.api.AbstractFetcher;
import okhttp3.HttpUrl;
import okhttp3.Request;

import java.io.IOException;
import java.io.InputStream;

@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceVariable extends IdentifiableArtefact {

    @JsonProperty
    private String shortName;
    @JsonProperty
    private String dataStructureComponentRole;
    @JsonProperty
    private String dataStructureComponentType;
    @JsonProperty
    private String identifierComponentIsComposite;
    @JsonProperty
    private String identifierComponentIsUnique;
    @JsonProperty
    private String representedVariable;
    @JsonProperty
    private String substantiveValueDomain;

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public String getDataStructureComponentRole() {
        return dataStructureComponentRole;
    }

    public void setDataStructureComponentRole(String dataStructureComponentRole) {
        this.dataStructureComponentRole = dataStructureComponentRole;
    }

    public String getDataStructureComponentType() {
        return dataStructureComponentType;
    }

    public void setDataStructureComponentType(String dataStructureComponentType) {
        this.dataStructureComponentType = dataStructureComponentType;
    }

    public String getIdentifierComponentIsComposite() {
        return identifierComponentIsComposite;
    }

    public void setIdentifierComponentIsComposite(String identifierComponentIsComposite) {
        this.identifierComponentIsComposite = identifierComponentIsComposite;
    }

    public String getIdentifierComponentIsUnique() {
        return identifierComponentIsUnique;
    }

    public void setIdentifierComponentIsUnique(String identifierComponentIsUnique) {
        this.identifierComponentIsUnique = identifierComponentIsUnique;
    }

    public String getRepresentedVariable() {
        return representedVariable;
    }

    public void setRepresentedVariable(String representedVariable) {
        this.representedVariable = representedVariable;
    }

    public String getSubstantiveValueDomain() {
        return substantiveValueDomain;
    }

    public void setSubstantiveValueDomain(String substantiveValueDomain) {
        this.substantiveValueDomain = substantiveValueDomain;
    }

    static class Fetcher extends AbstractFetcher<InstanceVariable> {
        
        @Override
        public InstanceVariable deserialize(ObjectMapper mapper, InputStream bytes) throws IOException {
            return mapper.readValue(bytes, InstanceVariable.class);
        }

        @Override
        public Request getRequest(HttpUrl prefix, String id, Long timestamp) {
            String normalizedId = id.replaceAll("InstanceVariable/", "");
            if (normalizedId.startsWith("/")) {
                normalizedId = normalizedId.substring(1);
            }

            Request.Builder builder = new Request.Builder();
            builder.header("Content-Type", "application/json");
            //HttpUrl base = HttpUrl.parse("https://lds.staging.ssbmod.net/ns/");
            HttpUrl url = prefix.resolve("./InstanceVariable/" + normalizedId);
            return builder.url(url).build();
        }
    }

}
