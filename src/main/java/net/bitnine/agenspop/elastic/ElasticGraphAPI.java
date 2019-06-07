package net.bitnine.agenspop.elastic;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;
import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import net.bitnine.agenspop.graph.structure.es.ElasticEdgeWrapper;
import net.bitnine.agenspop.graph.structure.es.ElasticTextSearchMode;
import net.bitnine.agenspop.graph.structure.es.ElasticTx;
import net.bitnine.agenspop.graph.structure.es.ElasticVertexWrapper;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ElasticGraphAPI {

    //////////////////////////////////////////////////
    //
    // common access services about ElasticElementWrapper
    //

    long countV();
    void deleteV(ElasticVertex vertex);
    ElasticVertex saveV(ElasticVertex vertex);
    Optional<? extends ElasticVertex> vertexOne(String uid);
    Iterable<? extends ElasticVertex> vertices();

    long countE();
    void deleteE(ElasticEdge edge);
    ElasticEdge saveE(ElasticEdge edge);
    Optional<? extends ElasticEdge> edgeOne(String uid);
    Iterable<? extends ElasticEdge> edges();

    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    Optional<? extends ElasticVertex> vertexByEid(Long eid);
    Optional<? extends ElasticVertex> vertexByEidAndDatasource(Long eid, String datasource);

    Iterable<? extends ElasticVertex> verticesByLabel(String label);
    Iterable<? extends ElasticVertex> verticesByLabelAndDatasource(String label, String datasource);
    Iterable<? extends ElasticVertex> verticesByPropsKey(String key);
    Iterable<? extends ElasticVertex> verticesByPropsValue(String value);
    Iterable<? extends ElasticVertex> verticesByPropsKeyAndValue(String key, String value);

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Optional<? extends ElasticEdge> edgeByEid(Long eid);
    Optional<? extends ElasticEdge> edgeByEidAndDatasource(Long eid, String datasource);

    Iterable<? extends ElasticEdge> edgesBySid(Long sid);
    Iterable<? extends ElasticEdge> edgesBySidAndDatasource(Long sid, String datasource);
    Iterable<? extends ElasticEdge> edgesByTid(Long tid);
    Iterable<? extends ElasticEdge> edgesByTidAndDatasource(Long tid, String datasource);
    Iterable<? extends ElasticEdge> edgesBySidAndTid(Long sid, Long tid);
    Iterable<? extends ElasticEdge> edgesBySidAndTidAndDatasource(Long sid, Long tid, String datasource);

    Iterable<? extends ElasticEdge> edgesByLabel(String label);
    Iterable<? extends ElasticEdge> edgesByLabelAndDatasource(String label, String datasource);
    Iterable<? extends ElasticEdge> edgesByPropsKey(String key);
    Iterable<? extends ElasticEdge> edgesByPropsValue(String value);
    Iterable<? extends ElasticEdge> edgesByPropsKeyAndValue(String key, String value);
/*
    ElasticVertex createNode(String... labels);
    ElasticVertex getNodeById(long id);
    ElasticEdge getRelationshipById(long id);

    Iterable<ElasticVertex> allNodes();
    Iterable<ElasticEdge> allRelationships();
    Iterable<ElasticVertex> findNodes(String label);
    Iterable<ElasticVertex> findNodes(String label, String property, Object value);
    Iterable<ElasticVertex> findNodes(String label, String property, String template, ElasticTextSearchMode searchMode);

    void shutdown();
    ElasticTx tx();
    Iterator<Map<String, Object>> execute(String query, Map<String, Object> params);
    boolean hasSchemaIndex(String label, String property);

    Iterable<String> getKeys();
    Object getProperty(String key);
    boolean hasProperty(String key);
    Object removeProperty(String key);
    void setProperty(String key, Object value);
 */
}
