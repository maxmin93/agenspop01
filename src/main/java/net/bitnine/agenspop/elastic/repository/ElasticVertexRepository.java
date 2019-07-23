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
public interface ElasticVertexRepository extends ElasticsearchRepository<ElasticVertexDocument, String> {

    // https://docs.spring.io/spring-data/jpa/docs/current/reference/html/#jpa.query-methods.query-creation

    // by ID list
    List<ElasticVertexDocument> findByIdIn(final List<String> ids);

    // by Datasource
    List<ElasticVertexDocument> findByDatasource(String datasource, Pageable pageable);
    List<ElasticVertexDocument> findByDatasourceAndIdIn(String datasource, final List<String> ids);
    List<ElasticVertexDocument> findByDatasourceAndLabelNotIn(String datasource, final List<String> labels, Pageable pageable);
    List<ElasticVertexDocument> findByDatasourceAndLabelAndIdIn(String datasource, String label, final List<String> ids);
    List<ElasticVertexDocument> findByDatasourceAndLabelAndIdNotIn(String datasource, String label, final List<String> ids, Pageable pageable);

    // by Label
    List<ElasticVertexDocument> findByLabel(String label, Pageable pageable);
    List<ElasticVertexDocument> findByLabelAndIdIn(String label, final List<String> ids);
    List<ElasticVertexDocument> findByLabelAndIdNotIn(String label, final List<String> ids, Pageable pageable);
    List<ElasticVertexDocument> findByLabelIn(final List<String> labels, Pageable pageable);
    List<ElasticVertexDocument> findByLabelNotIn(final List<String> labels, Pageable pageable);

    // case: ~label.eq
    // AND key.eq
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropertiesKey(
            String datasource, String label, String key, Pageable pageable);
    // AND key.eq AND value.eq
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValue(
            String datasource, String label, String key, String value, Pageable pageable);
    // AND key.within
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropertiesKeyIn(
            String datasource, String label, final List<String> keys, Pageable pageable);
    // AND key.eq AND values.within
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValueIn(
            String datasource, String label, String key, final List<String> valuesList, Pageable pageable);
    // AND none
    List<ElasticVertexDocument> findByDatasourceAndLabel(
            String datasource, String label, Pageable pageable);
    
    // case: none
    // key.eq
    List<ElasticVertexDocument> findByDatasourceAndPropertiesKey(
            String datasource, String key, Pageable pageable);
    // key.eq AND value.eq
    List<ElasticVertexDocument> findByDatasourceAndPropertiesKeyAndPropertiesValue(
            String datasource, String key, String value, Pageable pageable);
    // key.within
    List<ElasticVertexDocument> findByDatasourceAndPropertiesKeyIn(
            String datasource, final List<String> keys, Pageable pageable);
    // value.within
    List<ElasticVertexDocument> findByDatasourceAndPropertiesValueIn(
            String datasource, final List<String> values, Pageable pageable);
    // key.eq AND values.within
    List<ElasticVertexDocument> findByDatasourceAndPropertiesKeyAndPropertiesValueIn(
            String datasource, String key, final List<String> values, Pageable pageable);

    // case: ~label.within
    // AND none
    List<ElasticVertexDocument> findByDatasourceAndLabelIn(
            String datasource, List<String> labels, Pageable pageable);
    // AND key.eq
    List<ElasticVertexDocument> findByDatasourceAndPropertiesKeyAndLabelIn(
            String datasource, String key, List<String> labels, Pageable pageable);
    // AND key.eq AND value.eq
    List<ElasticVertexDocument> findByDatasourceAndPropertiesKeyAndPropertiesValueAndLabelIn(
            String datasource, String key, String value, List<String> labels, Pageable pageable);
    // AND key.within
    List<ElasticVertexDocument> findByDatasourceAndLabelInAndPropertiesKeyIn(
            String datasource, List<String> labels, List<String> keys, Pageable pageable);
    // AND values.within
    List<ElasticVertexDocument> findByDatasourceAndLabelInAndPropertiesValueIn(
            String datasource, List<String> labels, List<String> values, Pageable pageable);
    // AND key.eq AND values.within
    List<ElasticVertexDocument> findByDatasourceAndPropertiesKeyAndLabelInAndPropertiesValueIn(
            String datasource, String key, List<String> labels, List<String> values, Pageable pageable);

    // by Key or Value
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key, Pageable pageable);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.value\": \"?2\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsValueUsingCustomQuery(String datasource, String label, String value, Pageable pageable);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}, {\"match\": {\"properties.value\": \"?3\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(String datasource, String label, String key, String value, Pageable pageable);

    /////////////////////////////

    // List<ElasticVertexDocument> findByDatasourceAndIdNotIn(String datasource, final List<String> ids);
}
