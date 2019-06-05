package net.bitnine.agenspop.graph.structure.es;

import java.util.Iterator;
import java.util.Map;

public interface ElasticGraphWrapper {

    ElasticVertexWrapper createNode(String... labels);

    ElasticVertexWrapper getNodeById(long id);

    ElasticEdgeWrapper getRelationshipById(long id);

    void shutdown();

    Iterable<ElasticVertexWrapper> allNodes();

    Iterable<ElasticEdgeWrapper> allRelationships();

    Iterable<ElasticVertexWrapper> findNodes(String label);

    Iterable<ElasticVertexWrapper> findNodes(String label, String property, Object value);

    Iterable<ElasticVertexWrapper> findNodes(String label, String property, String template, ElasticTextSearchMode searchMode);

    ElasticTx tx();

    Iterator<Map<String, Object>> execute(String query, Map<String, Object> params);

    boolean hasSchemaIndex(String label, String property);

    Iterable<String> getKeys();

    Object getProperty(String key);

    boolean hasProperty(String key);

    Object removeProperty(String key);

    void setProperty(String key, Object value);
}
