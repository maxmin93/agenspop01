package net.bitnine.agenspop.elastic.model;

import net.bitnine.agenspop.elastic.document.ElasticPropertyDocument;

import java.util.Optional;
import java.util.Set;

public interface ElasticElement {

    String getId();
    String getLabel();
    String getDatasource();
    Iterable<String> getKeys();

    // ElasticProperty 만 반환하면 된다 ==> 실제 value 는 elasticProperty.value() 에서 처리
    Optional<ElasticProperty> getProperty(String name);
    ElasticProperty getProperty(String name, Object defaultValue);
    ElasticProperty setProperty(String name, Object value);
    ElasticProperty setProperty(String name, String type, Object value);
    boolean setProperty(ElasticProperty property);
    void removeProperty(String name);

    boolean hasProperty(String name);
    void delete();
    boolean isDeleted();

    Set<ElasticPropertyDocument> getProperties();
    void setProperties(Set<ElasticPropertyDocument> properties);

/*
    public default <V> Iterator<V> values(final String... propertyKeys) {
        return IteratorUtils.map(this.<V>properties(propertyKeys), property -> property.value());
    }
    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys);
*/

    /**
     * Common exceptions to use with an element.
     */
    public static class Exceptions {

        protected Exceptions() {
        }

        public static IllegalArgumentException providedKeyValuesMustBeAMultipleOfTwo() {
            return new IllegalArgumentException("The provided key/value array length must be a multiple of two");
        }

        public static IllegalArgumentException providedKeyValuesMustHaveALegalKeyOnEvenIndices() {
            return new IllegalArgumentException("The provided key/value array must have a String or T on even array indices");
        }

        public static IllegalStateException propertyAdditionNotSupported() {
            return new IllegalStateException("Property addition is not supported");
        }

        public static IllegalArgumentException labelCanNotBeNull() {
            return new IllegalArgumentException("Label can not be null");
        }

        public static IllegalArgumentException labelCanNotBeEmpty() {
            return new IllegalArgumentException("Label can not be empty");
        }

        public static IllegalArgumentException labelCanNotBeAHiddenKey(final String label) {
            return new IllegalArgumentException("Label can not be a hidden key: " + label);
        }
    }
}
