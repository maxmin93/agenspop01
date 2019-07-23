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
    List<ElasticEdgeDocument> findByDatasourceAndLabelNotIn(String datasource, final List<String> labels, Pageable pageable);
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndIdIn(String datasource, String label, final List<String> ids);
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndIdNotIn(String datasource, String label, final List<String> ids, Pageable pageable);

    // by Label
    List<ElasticEdgeDocument> findByLabel(String label, Pageable pageable);
    List<ElasticEdgeDocument> findByLabelAndIdIn(String label, final List<String> ids);
    List<ElasticEdgeDocument> findByLabelAndIdNotIn(String label, final List<String> ids, Pageable pageable);
    List<ElasticEdgeDocument> findByLabelIn(final List<String> labels, Pageable pageable);
    List<ElasticEdgeDocument> findByLabelNotIn(final List<String> labels, Pageable pageable);

    // case: ~label.eq
    // AND key.eq
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropertiesKey(
            String datasource, String label, String key, Pageable pageable);
    // AND key.eq AND value.eq
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValue(
            String datasource, String label, String key, String value, Pageable pageable);
    // AND key.within
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropertiesKeyIn(
            String datasource, String label, final List<String> keys, Pageable pageable);
    // AND key.eq AND values.within
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValueIn(
            String datasource, String label, String key, final List<String> valuesList, Pageable pageable);
    // AND none
    List<ElasticEdgeDocument> findByDatasourceAndLabel(
            String datasource, String label, Pageable pageable);

    // case: none
    // key.eq
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesKey(
            String datasource, String key, Pageable pageable);
    // key.eq AND value.eq
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesKeyAndPropertiesValue(
            String datasource, String key, String value, Pageable pageable);
    // key.within
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesKeyIn(
            String datasource, final List<String> keys, Pageable pageable);
    // value.within
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesValueIn(
            String datasource, final List<String> values, Pageable pageable);
    // key.eq AND values.within
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesKeyAndPropertiesValueIn(
            String datasource, String key, final List<String> values, Pageable pageable);

    // case: ~label.within
    // AND none
    List<ElasticEdgeDocument> findByDatasourceAndLabelIn(
            String datasource, List<String> labels, Pageable pageable);
    // AND key.eq
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesKeyAndLabelIn(
            String datasource, String key, List<String> labels, Pageable pageable);
    // AND key.eq AND value.eq
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesKeyAndPropertiesValueAndLabelIn(
            String datasource, String key, String value, List<String> labels, Pageable pageable);
    // AND key.within
    List<ElasticEdgeDocument> findByDatasourceAndLabelInAndPropertiesKeyIn(
            String datasource, List<String> labels, List<String> keys, Pageable pageable);
    // AND values.within
    List<ElasticEdgeDocument> findByDatasourceAndLabelInAndPropertiesValueIn(
            String datasource, List<String> labels, List<String> values, Pageable pageable);
    // AND key.eq AND values.within
    List<ElasticEdgeDocument> findByDatasourceAndPropertiesKeyAndLabelInAndPropertiesValueIn(
            String datasource, String key, List<String> labels, List<String> values, Pageable pageable);

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
