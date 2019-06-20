package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;

import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticEdgeRepository extends ElasticsearchRepository<ElasticEdgeDocument, String> {

    List<ElasticEdgeDocument> findByIdIn(List<String> ids);
    List<ElasticEdgeDocument> findByIdInAndDatasource(List<String> ids, String datasource);
    List<ElasticEdgeDocument> findByIdNotInAndLabel(List<String> ids, String label);
    List<ElasticEdgeDocument> findByIdNotInAndDatasource(List<String> ids, String datasource);
    List<ElasticEdgeDocument> findByIdNotInAndLabelAndDatasource(List<String> ids, String label, String datasource);

    List<ElasticEdgeDocument> findByLabel(String label);
    List<ElasticEdgeDocument> findByDatasource(String datasource);
    List<ElasticEdgeDocument> findByDatasourceAndLabel(String datasource, String label);

    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}, {\"match\": {\"properties.value\": \"?3\"}}]}}")
    List<ElasticEdgeDocument> findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(String datasource, String label, String key, String value);

    /////////////////////////////

    List<ElasticEdgeDocument> findBySid(String sid);
    List<ElasticEdgeDocument> findByTid(String tid);
    List<ElasticEdgeDocument> findBySidAndTid(String sid, String tid);

    /////////////////////////////

    // Page<ElasticEdgeDocument> findByLabel(String label, Pageable pageable);
    // Page<ElasticEdgeDocument> findByDatasource(String datasource, Pageable pageable);
}
