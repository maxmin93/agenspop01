package net.bitnine.agenspop.elastic;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ElasticGraphAPI {

    ElasticVertex createVertex(Integer eid, String datasource, String label);
    ElasticEdge createEdge(Integer eid, String datasource, String label, Integer sid, Integer tid);

    ElasticVertex saveVertex(ElasticVertex vertex);
    ElasticEdge saveEdge(ElasticEdge edge);

    ElasticVertex getVertexById(String datasource, Integer id);
    ElasticEdge getEdgeById(String datasource, Integer id);

    boolean hasSchemaIndex(String label);
    boolean hasSchemaIndex(String label, String key);

    ElasticTx tx();
    void shutdown();
    void startup();


    Iterable<? extends ElasticVertex> allVertices(String datasource);
    Iterable<? extends ElasticEdge> allEdges(String datasource);

    Iterable<? extends ElasticVertex> findVertices(String datasource, String label);
    Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key);
    Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key, Object value);

    // Iterable<ElasticVertex> findVertices(String label, String property, String template, ElasticStringSearchMode searchMode);
    // Iterator<Map<String, Object>> execute(String query, Map<String, Object> params);

    // Iterable<String> getKeys();
    // Object getProperty(String key);
    // boolean hasProperty(String key);
    // Object removeProperty(String key);
    // void setProperty(String key, Object value);
    
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

    Optional<? extends ElasticVertex> vertexByEid(Integer eid);
    Optional<? extends ElasticVertex> vertexByEidAndDatasource(Integer eid, String datasource);

    Iterable<? extends ElasticVertex> verticesByLabel(String label);
    Iterable<? extends ElasticVertex> verticesByLabelAndDatasource(String label, String datasource);
    Iterable<? extends ElasticVertex> verticesByPropsKey(String key);
    Iterable<? extends ElasticVertex> verticesByPropsValue(String value);
    Iterable<? extends ElasticVertex> verticesByPropsKeyAndValue(String key, String value);

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Optional<? extends ElasticEdge> edgeByEid(Integer eid);
    Optional<? extends ElasticEdge> edgeByEidAndDatasource(Integer eid, String datasource);

//    Iterable<ElasticEdge> edgesBySourceId(String sourceId);
//    Iterable<ElasticEdge> edgesByTargetId(String targetId);

    Iterable<? extends ElasticEdge> edgesBySid(Integer sid);
    Iterable<? extends ElasticEdge> edgesBySidAndDatasource(Integer sid, String datasource);
    Iterable<? extends ElasticEdge> edgesByTid(Integer tid);
    Iterable<? extends ElasticEdge> edgesByTidAndDatasource(Integer tid, String datasource);
    Iterable<? extends ElasticEdge> edgesBySidAndTid(Integer sid, Integer tid);
    Iterable<? extends ElasticEdge> edgesBySidAndTidAndDatasource(Integer sid, Integer tid, String datasource);

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
