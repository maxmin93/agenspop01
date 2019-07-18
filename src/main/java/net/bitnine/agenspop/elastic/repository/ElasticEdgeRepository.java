package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;

import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticEdgeRepository extends ElasticsearchRepository<ElasticEdgeDocument, String> {

    // https://docs.spring.io/spring-data/jpa/docs/current/reference/html/#jpa.query-methods.query-creation

    // by ID list
    List<ElasticEdgeDocument> findByIdIn(final List<String> ids);

    // by Datasource
    List<ElasticEdgeDocument> findByDatasource(String datasource, Pageable pageable);
    List<ElasticEdgeDocument> findByDatasourceAndIdIn(String datasource, final List<String> ids);

    // by Label
    List<ElasticEdgeDocument> findByLabel(String label, Pageable pageable);
    List<ElasticEdgeDocument> findByLabelAndIdIn(String label, final List<String> ids);
    List<ElasticEdgeDocument> findByLabelIn(final List<String> labels, Pageable pageable);

    // by Datasource and Label
    List<ElasticEdgeDocument> findByDatasourceAndLabel(String datasource, String label, Pageable pageable);
    List<ElasticEdgeDocument> findByDatasourceAndLabelIn(String datasource, final List<String> labels, Pageable pageable);
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndIdIn(String datasource, String label, final List<String> ids);

    // by Key or Value
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key, Pageable pageable);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.value\": \"?2\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsValueUsingCustomQuery(String datasource, String label, String value, Pageable pageable);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}, {\"match\": {\"properties.value\": \"?3\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(String datasource, String label, String key, String value, Pageable pageable);

    /////////////////////////////

    // by Sid or Tid
    List<ElasticEdgeDocument> findBySid(String sid, Pageable pageable);
    List<ElasticEdgeDocument> findByTid(String tid, Pageable pageable);
    List<ElasticEdgeDocument> findBySidAndTid(String sid, String tid, Pageable pageable);

    // by (Sid or Tid) and Labels
    List<ElasticEdgeDocument> findBySidAndLabelIn(String sid, final List<String> labels, Pageable pageable);
    List<ElasticEdgeDocument> findByTidAndLabelIn(String tid, final List<String> labels, Pageable pageable);
    List<ElasticEdgeDocument> findBySidAndTidAndLabelIn(String sid, String tid, final List<String> labels, Pageable pageable);

    List<ElasticEdgeDocument> findBySidAndLabelAndPropertiesKeyAndPropertiesValue(String sid, String label, String key, String value, Pageable pageable);
    List<ElasticEdgeDocument> findByTidAndLabelAndPropertiesKeyAndPropertiesValue(String tid, String label, String key, String value, Pageable pageable);

    /////////////////////////////

    // Page<ElasticEdgeDocument> findByLabel(String label, Pageable pageable);
    // Page<ElasticEdgeDocument> findByDatasource(String datasource, Pageable pageable);
    // List<ElasticEdgeDocument> findByDatasourceAndIdNotIn(String datasource, final List<String> ids);
}
