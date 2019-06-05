package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;

import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticEdgeRepository extends ElasticsearchRepository<ElasticEdgeDocument, String> {

//    Page<ElasticEdgeDocument> findByLabel(String label, Pageable pageable);
//    Page<ElasticEdgeDocument> findByDatasource(String datasource, Pageable pageable);

    List<ElasticEdgeDocument> findByEid(Long eid);
    List<ElasticEdgeDocument> findByEidAndDatasource(Long eid, String datasource);

    List<ElasticEdgeDocument> findByEidIn(List<Long> eid);
    List<ElasticEdgeDocument> findByEidInAndDatasource(List<Long> eid, String datasource);
    List<ElasticEdgeDocument> findByEidNotInAndLabel(List<Long> eid, String label);
    List<ElasticEdgeDocument> findByEidNotInAndDatasource(List<Long> eid, String datasource);
    List<ElasticEdgeDocument> findByEidNotInAndLabelAndDatasource(List<Long> eid, String label, String datasource);

    List<ElasticEdgeDocument> findByLabel(String label);
    List<ElasticEdgeDocument> findByLabelAndDatasource(String label, String datasource);

    List<ElasticEdgeDocument> findBySid(Long sid);
    List<ElasticEdgeDocument> findBySidAndDatasource(Long sid, String datasource);
    List<ElasticEdgeDocument> findByTid(Long tid);
    List<ElasticEdgeDocument> findByTidAndDatasource(Long tid, String datasource);
    List<ElasticEdgeDocument> findBySidAndTid(Long sid, Long tid);
    List<ElasticEdgeDocument> findBySidAndTidAndDatasource(Long sid, Long tid, String datasource);

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
