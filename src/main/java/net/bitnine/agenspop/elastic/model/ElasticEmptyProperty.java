package net.bitnine.agenspop.elastic.model;

import java.util.NoSuchElementException;

public final class ElasticEmptyProperty implements ElasticProperty {

    private static final ElasticEmptyProperty INSTANCE = new ElasticEmptyProperty();
    private static final String EMPTY_PROPERTY = "p[empty]";

    private ElasticEmptyProperty() {
    }

    @Override
    public String getKey() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public String getType() throws NoSuchElementException {
        throw Exceptions.propertyDoesNotExist();
    }
    @Override
    public String getValue() throws NoSuchElementException {
        throw Exceptions.propertyDoesNotExist();
    }
    @Override
    public String value() throws NoSuchElementException {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public String toString() {
        return EMPTY_PROPERTY;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return object instanceof ElasticEmptyProperty;
    }

    public int hashCode() {
        return 1281483122;
    }

    public static ElasticProperty instance() {
        return INSTANCE;
    }
}
