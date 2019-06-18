package net.bitnine.agenspop.elastic.model;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.list.UnmodifiableList;

import java.lang.reflect.Constructor;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public interface ElasticProperty {

    static List<String> whiteList = Arrays.asList(
            String.class.getName(), Integer.class.getName(), Long.class.getName()
            , Float.class.getName(), Double.class.getName(), Boolean.class.getName()
            // ** excpetions
            // List.class.getName(), Map.class.getName(), Set.class.getName()
    );

    // String elementId();
    String getKey();
    String getType();
    String getValue() throws NoSuchElementException;

    default boolean isPresent() {
        try{
            Object value = this.value();
            return value != null;
        }catch (NoSuchElementException ex){
            return false;
        }
    }

    default Object value() throws NoSuchElementException {
        Object value = null;
        if( !whiteList.contains(getType()) )
            throw new NoSuchElementException("Collection Types cannot be supported in Property");

        try{
            Class cls = Class.forName( getType() );
            Constructor cons = cls.getDeclaredConstructor( String.class );
            value = cons.newInstance( getValue() );
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                | IllegalAccessException | InvocationTargetException ex){
            throw new NoSuchElementException(String.format("property.value(\"%s\")<%s> exception: %s"
                    , getValue(), getType(), ex.toString()));
        }
        return value;
    }

    static ElasticProperty empty() {
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
