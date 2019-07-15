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

    // by Label
    List<ElasticVertexDocument> findByLabel(String label, Pageable pageable);
    List<ElasticVertexDocument> findByLabelAndIdIn(String label, final List<String> ids);
    List<ElasticVertexDocument> findByLabelIn(final List<String> labels, Pageable pageable);

    // by Datasource and Label
    List<ElasticVertexDocument> findByDatasourceAndLabel(String datasource, String label, Pageable pageable);
    List<ElasticVertexDocument> findByDatasourceAndLabelIn(String datasource, final List<String> labels, Pageable pageable);
    List<ElasticVertexDocument> findByDatasourceAndLabelAndIdIn(String datasource, String label, final List<String> ids);

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
