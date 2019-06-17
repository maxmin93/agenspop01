package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticProperty;

public class ElasticPropertyDocument implements ElasticProperty {

    // **NOTE: private 설정시 SerializationFeature.FAIL_ON_EMPTY_BEANS
    private String elementId;      // ID ==> datasource + "::" + eid
    private String key;
    private String type;
    private String value;

    public ElasticPropertyDocument(){}
    public ElasticPropertyDocument(final String elementId, final String key, final String type, final Object value) {
        this.elementId = elementId;
        this.key = key;
        this.type = type;
        this.value = value.toString();
    }

    @Override public String elementId() { return this.elementId; }
    @Override public String getKey() { return this.key; }
    @Override public String getType() { return this.type; }
    @Override public String getValue() { return this.value; }

    @Override
    public String toString() {
        return String.format("%s<%s>=%s", key, type, value);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) return false;
        if (this.getClass() != obj.getClass()) return false;
        if (this == obj) return true;

        ElasticPropertyDocument that = (ElasticPropertyDocument) obj;
        if (this.key == null || that.getKey() == null || !this.key.equals(that.getKey()) )
            return false;
        if (this.value == null || that.getValue() == null || !this.value.equals(that.getValue()))
            return false;
        if (this.type == null || that.getType() == null || !this.type.equals(that.getType()) )
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return 31*value.hashCode() + 43*type.hashCode() + 59*key.hashCode();
    }

}
