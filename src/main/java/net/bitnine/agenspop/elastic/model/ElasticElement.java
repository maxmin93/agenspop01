package net.bitnine.agenspop.elastic.model;

public interface ElasticElement {

    Long getEid();
    String getLabel();
    String getDatasource();
    Iterable<String> keys();
//    Object getProperty(String name);
//    Object getProperty(String name, Object defaultValue);
//    void setProperty(String name, Object value);
//    Object removeProperty(String name);
    boolean hasProperty(String name);
    void delete();
    boolean isDeleted();

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