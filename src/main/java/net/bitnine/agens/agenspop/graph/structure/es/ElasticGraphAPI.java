package net.bitnine.agens.agenspop.graph.structure.es;

import java.util.Iterator;
import java.util.Map;

public interface ElasticGraphAPI {

    ElasticVertex createNode(String... labels);

    ElasticVertex getNodeById(long id);

    ElasticEdge getRelationshipById(long id);

    void shutdown();

    Iterable<ElasticVertex> allNodes();

    Iterable<ElasticEdge> allRelationships();

    Iterable<ElasticVertex> findNodes(String label);

    Iterable<ElasticVertex> findNodes(String label, String property, Object value);

    Iterable<ElasticVertex> findNodes(String label, String property, String template, ElasticTextSearchMode searchMode);

    ElasticTx tx();

    Iterator<Map<String, Object>> execute(String query, Map<String, Object> params);

    boolean hasSchemaIndex(String label, String property);

    Iterable<String> getKeys();

    Object getProperty(String key);

    boolean hasProperty(String key);

    Object removeProperty(String key);

    void setProperty(String key, Object value);
}
