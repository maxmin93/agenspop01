package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;

import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticEdgeRepository extends ElasticsearchRepository<ElasticEdgeDocument, String> {

//    Page<ElasticEdgeDocument> findByLabel(String label, Pageable pageable);
//    Page<ElasticEdgeDocument> findByDatasource(String datasource, Pageable pageable);

    List<ElasticEdgeDocument> findByDatasource(String datasource);

    List<ElasticEdgeDocument> findByEid(Integer eid);
    List<ElasticEdgeDocument> findByEidAndDatasource(Integer eid, String datasource);

    List<ElasticEdgeDocument> findByEidIn(List<Integer> eid);
    List<ElasticEdgeDocument> findByEidInAndDatasource(List<Integer> eid, String datasource);
    List<ElasticEdgeDocument> findByEidNotInAndLabel(List<Integer> eid, String label);
    List<ElasticEdgeDocument> findByEidNotInAndDatasource(List<Integer> eid, String datasource);
    List<ElasticEdgeDocument> findByEidNotInAndLabelAndDatasource(List<Integer> eid, String label, String datasource);

    List<ElasticEdgeDocument> findByLabel(String label);
    List<ElasticEdgeDocument> findByLabelAndDatasource(String label, String datasource);

    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"props.key\": \"?2\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key);

    List<ElasticEdgeDocument> findBySid(Integer sid);
    List<ElasticEdgeDocument> findBySidAndDatasource(Integer sid, String datasource);
    List<ElasticEdgeDocument> findByTid(Integer tid);
    List<ElasticEdgeDocument> findByTidAndDatasource(Integer tid, String datasource);
    List<ElasticEdgeDocument> findBySidAndTid(Integer sid, Integer tid);
    List<ElasticEdgeDocument> findBySidAndTidAndDatasource(Integer sid, Integer tid, String datasource);

    /////////////////////////////

    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.key\": \"?0\"}}]}}")
    List<ElasticEdgeDocument> findByPropsKeyUsingCustomQuery(String key);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.value\": \"?0\"}}]}}")
    List<ElasticEdgeDocument> findByPropsValueUsingCustomQuery(String value);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.key\": \"?0\"}}, {\"match\": {\"props.value\": \"?1\"}}]}}")
    List<ElasticEdgeDocument> findByPropsKeyAndValueUsingCustomQuery(String key, String value);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.key\": \"?0\"}}, {\"match\": {\"props.value\": \"?1\"}}, {\"match\": {\"props.type\": \"?2\"}}]}}")
    List<ElasticEdgeDocument> findByPropsKeyAndValueAndTypeUsingCustomQuery(String key, String value, String type);

}
