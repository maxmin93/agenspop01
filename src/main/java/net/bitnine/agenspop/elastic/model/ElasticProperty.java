package net.bitnine.agenspop.elastic.model;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public interface ElasticProperty {

    public String key();
    public String type();
    public String value() throws NoSuchElementException;

    public default String guessType(){
        Object v = (Object) value();

        if( v instanceof String ) return String.class.getSimpleName();

        else if( v instanceof Integer ) return Integer.class.getSimpleName();
        else if( v instanceof Long ) return Long.class.getSimpleName();
        else if( v instanceof Float ) return Float.class.getSimpleName();
        else if( v instanceof Double ) return Double.class.getSimpleName();
        else if( v instanceof Boolean ) return Boolean.class.getSimpleName();

        else if( v instanceof Date) return Date.class.getSimpleName();

        else if( v instanceof List) return List.class.getSimpleName();
        else if( v instanceof Map) return Map.class.getSimpleName();

        return "null";        // Exception is better
    }

    public String element();
    public static ElasticProperty empty() {
        return ElasticEmptyProperty.instance();
    }

    /**
     * Common exceptions to use with a property.
     */
    public static class Exceptions {

        private Exceptions() {
        }

        public static IllegalArgumentException propertyKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Property key can not be the empty string");
        }

        public static IllegalArgumentException propertyKeyCanNotBeNull() {
            return new IllegalArgumentException("Property key can not be null");
        }

        public static IllegalArgumentException propertyValueCanNotBeNull() {
            return new IllegalArgumentException("Property value can not be null");
        }

        public static IllegalArgumentException propertyKeyCanNotBeAHiddenKey(final String key) {
            return new IllegalArgumentException("Property key can not be a hidden key: " + key);
        }

        public static IllegalStateException propertyDoesNotExist() {
            return new IllegalStateException("The property does not exist as it has no key, value, or associated elementId");
        }

        public static IllegalStateException propertyDoesNotExist(final String element, final String key) {
            return new IllegalStateException("The property does not exist as the key has no associated value for the provided elementId: " + element + ":" + key);
        }

        public static IllegalArgumentException dataTypeOfPropertyValueNotSupported(final Object val) {
            return dataTypeOfPropertyValueNotSupported(val, null);
        }

        public static IllegalArgumentException dataTypeOfPropertyValueNotSupported(final Object val, final Exception rootCause) {
            return new IllegalArgumentException(String.format("Property value [%s] is of type %s is not supported", val, val.getClass()), rootCause);
        }

        public static IllegalStateException propertyRemovalNotSupported() {
            return new IllegalStateException("Property removal is not supported");
        }
    }
}
