package net.bitnine.agenspop.elastic;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

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

    boolean existsVertex(String id);
    boolean existsEdge(String id);

    Optional<? extends ElasticVertex> getVertexById(String id);
    Optional<? extends ElasticEdge> getEdgeById(String id);

    ElasticVertex createVertex(String id, String label);
    ElasticEdge createEdge(String id, String label, String sid, String tid);

    ElasticVertex saveVertex(ElasticVertex vertex);
    ElasticEdge saveEdge(ElasticEdge edge);

    void deleteVertex(ElasticVertex vertex);
    void deleteEdge(ElasticEdge edge);

    long countV();
    long countE();

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

    boolean removeDatasource(String datasource);

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

    // **TIP: remove duplicates from a list
    //     0) new ArrayList<>(new HashSet<>(Arrays.asList( listWithDuplicates )))   -- plain Java
    //     1) Lists.newArrayList(Sets.newHashSet(listWithDuplicates));              -- using Guava
    //     2) listWithDuplicates.stream().distinct().collect(Collectors.toList());  -- using Stream

    Iterable<ElasticVertex> findVertices(String... ids);
    Iterable<ElasticVertex> findVertices(String datasource);
    Iterable<ElasticVertex> findVertices(String datasource, String label);
    Iterable<ElasticVertex> findVertices(String datasource, String label, String key);
    Iterable<ElasticVertex> findVertices(String datasource, String label, String key, Object value);

    ElasticVertex findOtherVertexOfEdge(String eid, String vid);
    Iterable<ElasticVertex> findNeighborVertices(String id, Direction direction, final String... labels);

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Iterable<ElasticEdge> findEdges(String... ids);
    Iterable<ElasticEdge> findEdges(String datasource);
    Iterable<ElasticEdge> findEdges(String datasource, String label);
    Iterable<ElasticEdge> findEdges(String datasource, String label, String key);
    Iterable<ElasticEdge> findEdges(String datasource, String label, String key, Object value);

    Iterable<ElasticEdge> findEdgesBySid(String sid);
    Iterable<ElasticEdge> findEdgesByTid(String tid);
    Iterable<ElasticEdge> findEdgesBySidAndTid(String sid, String tid);

    Iterable<ElasticEdge> findEdgesOfVertex(String id, Direction direction, final String... labels);

}
