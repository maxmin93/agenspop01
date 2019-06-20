package net.bitnine.agenspop.elastic;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ElasticGraphAPI {

    //////////////////////////////////////////////////
    //
    // common access services about ElasticElement
    //

    Iterable<? extends ElasticVertex> vertices();
    Iterable<? extends ElasticEdge> edges();

    Optional<? extends ElasticVertex> getVertexById(String id);
    Optional<? extends ElasticEdge> getEdgeById(String id);

    ElasticVertex createVertex(String id, String label);
    ElasticEdge createEdge(String id, String label, String sid, String tid);

    ElasticVertex saveVertex(ElasticVertex vertex);
    ElasticEdge saveEdge(ElasticEdge edge);

    void deleteVertex(ElasticVertex vertex);
    void deleteEdge(ElasticEdge edge);

    //////////////////////////////////////////////////
    //
    // Admin
    //

    ElasticTx tx();
    void shutdown();
    void startup();

    //////////////////////////////////////////////////
    //
    // about Schema
    //

    long countV();
    long countE();

    long countV(String datasource);
    long countE(String datasource);

    boolean hasSchema(String label);
    boolean hasSchema(String label, String key);

    Map<String, Long> listVertexDatasources();
    Map<String, Long> listEdgeDatasources();

    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    Iterable<? extends ElasticVertex> findVertices(String datasource);
    Iterable<? extends ElasticVertex> findVertices(String datasource, String label);
    Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key);
    Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key, Object value);

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Iterable<? extends ElasticEdge> findEdgesBySid(String sid);
    Iterable<? extends ElasticEdge> findEdgesByTid(String tid);
    Iterable<? extends ElasticEdge> findEdgesBySidAndTid(String sid, String tid);

    Iterable<? extends ElasticEdge> findEdges(String datasource);
    Iterable<? extends ElasticEdge> findEdges(String datasource, String label);
    Iterable<? extends ElasticEdge> findEdges(String datasource, String label, String key);
    Iterable<? extends ElasticEdge> findEdges(String datasource, String label, String key, Object value);
}
