package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticProperty;

public class ElasticPropertyDocument implements ElasticProperty {

    // **NOTE: private 설정시 SerializationFeature.FAIL_ON_EMPTY_BEANS
    public String element;
    public String key;
    public String type;
    public String value;

    public ElasticPropertyDocument(){}
    public ElasticPropertyDocument(final String element, final String key, final Object value) {
        this.element = element;
        this.key = key;
        this.type = value.getClass().getSimpleName();
        this.value = value.toString();
    }

    @Override public String element() { return this.element; }
    @Override public String key() { return this.key; }
    @Override public String type() { return this.type; }
    @Override public String value() { return this.value; }

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
        if (this.key == null || that.key() == null || !this.key.equals(that.key()) )
            return false;
        if (this.value == null || that.value() == null || !this.type().equals(that.type())
                || !this.value.equals(that.value()) )
            return false;
        if (this.type == null || that.type() == null || !this.type.equals(that.type()) )
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return 31*value.hashCode() + 43*type().hashCode() + 59*key.hashCode();
    }

}
