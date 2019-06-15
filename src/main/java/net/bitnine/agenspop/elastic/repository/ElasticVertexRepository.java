package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;

import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticVertexRepository extends ElasticsearchRepository<ElasticVertexDocument, String> {

    List<ElasticVertexDocument> findByDatasource(String datasource);

    List<ElasticVertexDocument> findByEid(Integer eid);
    List<ElasticVertexDocument> findByEidAndDatasource(Integer eid, String datasource);

    List<ElasticVertexDocument> findByEidIn(List<Integer> eid);
    List<ElasticVertexDocument> findByEidInAndDatasource(List<Integer> eid, String datasource);
    List<ElasticVertexDocument> findByEidNotInAndLabel(List<Integer> eid, String label);
    List<ElasticVertexDocument> findByEidNotInAndDatasource(List<Integer> eid, String datasource);
    List<ElasticVertexDocument> findByEidNotInAndLabelAndDatasource(List<Integer> eid, String label, String datasource);

    List<ElasticVertexDocument> findByLabel(String label);
    List<ElasticVertexDocument> findByLabelAndDatasource(String label, String datasource);

    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}, {\"match\": {\"properties.value\": \"?3\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(String datasource, String label, String key, String value);

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
