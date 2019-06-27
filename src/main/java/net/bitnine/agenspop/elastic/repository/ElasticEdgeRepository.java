package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;

import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticEdgeRepository extends ElasticsearchRepository<ElasticEdgeDocument, String> {

    // https://docs.spring.io/spring-data/jpa/docs/current/reference/html/#jpa.query-methods.query-creation

    List<ElasticEdgeDocument> findByDatasource(String datasource);

    List<ElasticEdgeDocument> findByIdIn(final List<String> ids);
    List<ElasticEdgeDocument> findByDatasourceAndIdIn(String datasource, final List<String> ids);
    List<ElasticEdgeDocument> findByIdNotInAndLabel(String label, final List<String> ids);
    List<ElasticEdgeDocument> findByIdNotInAndDatasource(String datasource, final List<String> ids);
    List<ElasticEdgeDocument> findByIdNotInAndLabelAndDatasource(String label, String datasource, final List<String> ids);

    List<ElasticEdgeDocument> findByLabel(String label);
    List<ElasticEdgeDocument> findByLabelIn(final List<String> labels);

    List<ElasticEdgeDocument> findByDatasourceAndLabel(String datasource, String label);
    List<ElasticEdgeDocument> findByDatasourceAndLabelIn(String datasource, final List<String> labels);

    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}, {\"match\": {\"properties.value\": \"?3\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(String datasource, String label, String key, String value);

    /////////////////////////////

    List<ElasticEdgeDocument> findBySid(String sid);
    List<ElasticEdgeDocument> findByTid(String tid);
    List<ElasticEdgeDocument> findBySidAndTid(String sid, String tid);

    List<ElasticEdgeDocument> findBySidAndLabelIn(String sid, final List<String> labels);
    List<ElasticEdgeDocument> findByTidAndLabelIn(String tid, final List<String> labels);
    List<ElasticEdgeDocument> findBySidAndTidAndLabelIn(String sid, String tid, final List<String> labels);

    /////////////////////////////

    // Page<ElasticEdgeDocument> findByLabel(String label, Pageable pageable);
    // Page<ElasticEdgeDocument> findByDatasource(String datasource, Pageable pageable);
}
