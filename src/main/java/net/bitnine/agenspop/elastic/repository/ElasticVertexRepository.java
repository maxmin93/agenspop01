package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;

import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticVertexRepository extends ElasticsearchRepository<ElasticVertexDocument, String> {

    List<ElasticVertexDocument> findByEid(Long eid);
    List<ElasticVertexDocument> findByEidAndDatasource(Long eid, String datasource);

    List<ElasticVertexDocument> findByEidIn(List<Long> eid);
    List<ElasticVertexDocument> findByEidInAndDatasource(List<Long> eid, String datasource);
    List<ElasticVertexDocument> findByEidNotInAndLabel(List<Long> eid, String label);
    List<ElasticVertexDocument> findByEidNotInAndDatasource(List<Long> eid, String datasource);
    List<ElasticVertexDocument> findByEidNotInAndLabelAndDatasource(List<Long> eid, String label, String datasource);

    List<ElasticVertexDocument> findByLabel(String label);
    List<ElasticVertexDocument> findByLabelAndDatasource(String label, String datasource);

    /////////////////////////////

    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.key\": \"?0\"}}]}}")
    List<ElasticVertexDocument> findByPropsKeyUsingCustomQuery(String key);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.value\": \"?0\"}}]}}")
    List<ElasticVertexDocument> findByPropsValueUsingCustomQuery(String value);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.key\": \"?0\"}}, {\"match\": {\"props.value\": \"?1\"}}]}}")
    List<ElasticVertexDocument> findByPropsKeyAndValueUsingCustomQuery(String key, String value);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"props.key\": \"?0\"}}, {\"match\": {\"props.value\": \"?1\"}}, {\"match\": {\"props.type\": \"?2\"}}]}}")
    List<ElasticVertexDocument> findByPropsKeyAndValueAndTypeUsingCustomQuery(String key, String value, String type);

}
