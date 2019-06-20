package net.bitnine.agenspop.elastic.repository;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;
import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;

import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ElasticVertexRepository extends ElasticsearchRepository<ElasticVertexDocument, String> {


    List<ElasticVertexDocument> findByIdIn(List<String> ids);
    List<ElasticVertexDocument> findByIdInAndDatasource(List<String> ids, String datasource);
    List<ElasticVertexDocument> findByIdNotInAndLabel(List<String> ids, String label);
    List<ElasticVertexDocument> findByIdNotInAndDatasource(List<String> ids, String datasource);
    List<ElasticVertexDocument> findByIdNotInAndLabelAndDatasource(List<String> ids, String label, String datasource);

    List<ElasticVertexDocument> findByLabel(String label);
    List<ElasticVertexDocument> findByDatasource(String datasource);
    List<ElasticVertexDocument> findByDatasourceAndLabel(String datasource, String label);

    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(String datasource, String label, String key);
    @Query("{\"bool\": {\"must\": [{\"match\": {\"datasouce\": \"?0\"}}, {\"match\": {\"label\": \"?1\"}}, {\"match\": {\"properties.key\": \"?2\"}}, {\"match\": {\"properties.value\": \"?3\"}}]}}")
    List<ElasticVertexDocument> findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(String datasource, String label, String key, String value);

    /////////////////////////////

}
