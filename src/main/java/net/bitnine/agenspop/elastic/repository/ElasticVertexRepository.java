package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;
import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;

import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticVertexRepository extends ElasticsearchRepository<ElasticVertexDocument, String> {

    // https://docs.spring.io/spring-data/jpa/docs/current/reference/html/#jpa.query-methods.query-creation

    List<ElasticVertexDocument> findByDatasource(String datasource);

    List<ElasticVertexDocument> findByIdIn(final List<String> ids);
    List<ElasticVertexDocument> findByDatasourceAndIdIn(String datasource, final List<String> ids);
    List<ElasticVertexDocument> findByDatasourceAndIdNotIn(String datasource, final List<String> ids);

    List<ElasticVertexDocument> findByLabelAndIdIn(String label, final List<String> ids);
    List<ElasticVertexDocument> findByLabelAndIdNotIn(String label, final List<String> ids);
    List<ElasticVertexDocument> findByDatasourceAndLabelAndIdNotIn(String datasource, String label, final List<String> ids);

    List<ElasticVertexDocument> findByLabel(String label);
    List<ElasticVertexDocument> findByDatasourceAndLabel(String datasource, String label);

    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}, {\"match\": {\"properties.value\": \"?3\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(String datasource, String label, String key, String value);

    /////////////////////////////

}
