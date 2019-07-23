package net.bitnine.agenspop.elastic;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ElasticGraphAPI {

    // **NOTE: 탐색 규칙
    //      0) id
    //          => 무조건, 유일한, 절대적인 구분자
    //      1) datasource
    //          => 유일하다. 전체 대상으로 label 또는 value 를 검색하기도 함
    //      2) label
    //          => 멀티 검색도 가능
    //      3) property.value
    //          => property 대상으로 검색
    //      ...
    //      9) 전체 대상 검색
    //          기본 graph 를 설정 : 'whole' <== 대상 질의시 datasource 를 제외하고 검색하도록

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

    //  **NOTE: optimized find functions
    //      ==> http://tinkerpop.apache.org/docs/current/reference/#has-step
    //    has(key,value)           : findVerticesWithKV(ds, key, val)
    //    has(label, key, value)   : findVerticesWithLKV(ds, label, key, value)
    //    hasLabel(labels…​)        : findVerticesWithLabels(ds, ...labels)
    //    hasId(ids…​)              : findVertices(ids)
    //    hasKey(keys…​)            : findVerticesWithKeys(ds, ...keys)
    //    hasValue(values…​)        : findVerticesWithValues(ds, ...values)
    //    has(key)                 : findVerticesWithKey(ds, key)
    //    hasNot(key)              : findVerticesWithNotKey(ds, key)

    Iterable<ElasticVertex> findVertices(String... ids);
    Iterable<ElasticVertex> findVertices(String datasource);
    Iterable<ElasticVertex> findVertices(String datasource, String label);
    Iterable<ElasticVertex> findVertices(String datasource, String... ids);
    Iterable<ElasticVertex> findVertices(String datasource, List<String> labels, List<String> keys, List<Object> values);

    ElasticVertex findOtherVertexOfEdge(String eid, String vid);
    Iterable<ElasticVertex> findNeighborVertices(String id, Direction direction, final String... labels);

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    Iterable<ElasticEdge> findEdges(String... ids);
    Iterable<ElasticEdge> findEdges(String datasource);
    Iterable<ElasticEdge> findEdges(String datasource, String label);
    Iterable<ElasticEdge> findEdges(String datasource, String... ids);
    Iterable<ElasticEdge> findEdges(String datasource, List<String> labels, List<String> keys, List<Object> values);

    Iterable<ElasticEdge> findEdgesBySid(String sid);
    Iterable<ElasticEdge> findEdgesByTid(String tid);
    Iterable<ElasticEdge> findEdgesBySidAndTid(String sid, String tid);

    Iterable<ElasticEdge> findEdgesOfVertex(String id, Direction direction, final String... labels);
    Iterable<ElasticEdge> findEdgesOfVertex(String id, Direction direction, String label, String key, Object value);

}
