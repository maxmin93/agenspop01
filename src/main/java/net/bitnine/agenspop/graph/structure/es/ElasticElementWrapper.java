package net.bitnine.agenspop.graph.structure.es;

public interface ElasticElementWrapper {

    long getId();
    Iterable<String> getKeys();
    Object getProperty(String name);
    Object getProperty(String name, Object defaultValue);
    void setProperty(String name, Object value);
    Object removeProperty(String name);
    boolean hasProperty(String name);
    void delete();
}