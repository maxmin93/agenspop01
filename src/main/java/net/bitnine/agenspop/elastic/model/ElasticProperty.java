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

            // **NOTE: 당장은 단일 객체만 처리한다
            //
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
        // **NOTE: logstash 로부터 데이터가 변경되어 유입되기 때문에 타입체크 안해도 됨
        // if( !whiteList.contains(getType()) )
        //     throw new NoSuchElementException("Collection Types cannot be supported in Property");

        // **NOTE: 좀더 스마트한 객체 생성 방법이 필요함
        //      지금은 constructor(String valueStr) 형태로만 작동
        Object value = null;
        try{
            Class cls = Class.forName( getType() );
            Constructor cons = cls.getDeclaredConstructor( String.class );
            value = cons.newInstance( getValue() );
//            if( cons != null ) value = cons.newInstance( getValue() );
//            value = getValue();
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                | IllegalAccessException | InvocationTargetException ex){
            // 처리 불가할 경우, String 타입으로 리턴
            value = getValue();
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
