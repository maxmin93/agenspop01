package net.bitnine.agenspop.elasticgraph.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import net.bitnine.agenspop.elasticgraph.model.ElasticEdge;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

@Slf4j
public final class ElasticEdgeService extends ElasticElementService {

    private final String INDEX;

    public ElasticEdgeService(
            RestHighLevelClient client,     // elasticsearch config
            ObjectMapper mapper             // spring boot web starter
    ) {
        super(client, mapper);
        this.INDEX = ElasticGraphService.INDEX_EDGE;
    }

    ///////////////////////////////////////////////////////////////

    public long count() throws Exception {
        return super.count(INDEX);
    }

    public long count(String datasource) throws Exception {
        return super.count(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

    public String createDocument(ElasticEdge document) throws Exception {
        return super.createDocument(INDEX, ElasticEdge.class, document);
    }

    public String updateDocument(ElasticEdge document) throws Exception {
        return super.updateDocument(INDEX, ElasticEdge.class, document);
    }

    public String deleteDocument(String id) throws Exception {
        return super.deleteDocument(INDEX, id);
    }
    public long deleteDocuments(String datasource) throws Exception {
        return super.deleteDocuments(INDEX, datasource);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findAll() throws Exception {
        return super.findAll(INDEX, ElasticEdge.class);
    }

    public ElasticEdge findById(String id) throws Exception {
        return super.findById(INDEX, ElasticEdge.class, id);
    }

    public boolean existsId(String id) throws Exception {
        return super.existsId(INDEX, id);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findByIds(String[] ids) throws Exception {
        return super.findByIds(INDEX, ElasticEdge.class, ids);
    }

    public List<ElasticEdge> findByLabel(int size, String label) throws Exception {
        return super.findByLabel(INDEX, ElasticEdge.class, size, label);
    }

    public List<ElasticEdge> findByDatasource(int size, String datasource) throws Exception {
        return super.findByDatasource(INDEX, ElasticEdge.class, size, datasource);
    }

    public List<ElasticEdge> findByDatasourceAndLabel(int size, String datasource, String label) throws Exception {
        return super.findByDatasourceAndLabel(INDEX, ElasticEdge.class, size, datasource, label);
    }
    public List<ElasticEdge> findByDatasourceAndLabels(int size, String datasource, final String[] labels) throws Exception {
        return super.findByDatasourceAndLabels(INDEX, ElasticEdge.class, size, datasource, labels);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeys(int size, String datasource, final String[] keys) throws Exception{
        return super.findByDatasourceAndPropertyKeys(INDEX, ElasticEdge.class, size, datasource, keys);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKey(int size, String datasource, String key) throws Exception{
        return super.findByDatasourceAndPropertyKey(INDEX, ElasticEdge.class, size, datasource, key);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeyNot(int size, String datasource, String keyNot) throws Exception{
        return super.findByDatasourceAndPropertyKeyNot(INDEX, ElasticEdge.class, size, datasource, keyNot);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyValues(int size, String datasource, final String[] values) throws Exception{
        return super.findByDatasourceAndPropertyValues(INDEX, ElasticEdge.class, size, datasource, values);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyValue(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValue(INDEX, ElasticEdge.class, size, datasource, value);
    }
    public List<ElasticEdge> findByDatasourceAndPropertyValuePartial(int size, String datasource, String value) throws Exception{
        return super.findByDatasourceAndPropertyValuePartial(INDEX, ElasticEdge.class, size, datasource, value);
    }

    public List<ElasticEdge> findByDatasourceAndPropertyKeyValue(int size, String datasource, String key, String value) throws Exception{
        return super.findByDatasourceAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, key, value);
    }

    public List<ElasticEdge> findByDatasourceAndLabelAndPropertyKeyValue(int size, String datasource, String label, String key, String value) throws Exception{
        return super.findByDatasourceAndLabelAndPropertyKeyValue(INDEX, ElasticEdge.class, size, datasource, label, key, value);
    }
    public List<ElasticEdge> findByDatasourceAndLabelAndPropertyKeyValues(int size, String datasource, String label, Map<String,String> kvPairs) throws Exception{
        return super.findByDatasourceAndLabelAndPropertyKeyValues(INDEX, ElasticEdge.class, size, datasource, label, kvPairs);
    }

    public List<ElasticEdge> findByHasContainers(int size, String datasource
            , String label, final String[] labels
            , String key, String keyNot, final String[] keys
            , final String[] values, final Map<String,String> kvPairs) throws Exception {
        return super.findByHasContainers(INDEX, ElasticEdge.class, size, datasource
                , label, labels, key, keyNot, keys, values, kvPairs);
    }

    ///////////////////////////////////////////////////////////////

    public List<ElasticEdge> findByDatasourceAndDirection(
            int size, String datasource, String vid, Direction direction) throws Exception{
        // define : nested query
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(termQuery("datasource", datasource));
        // with direction
        if( direction.equals(Direction.IN))
            queryBuilder = queryBuilder.must(termQuery("tid", vid));
        else if( direction.equals(Direction.OUT))
            queryBuilder = queryBuilder.must(termQuery("sid", vid));
        else{
            queryBuilder = queryBuilder.should(termQuery("tid", vid));
            queryBuilder = queryBuilder.should(termQuery("sid", vid));
        }
        // search
        return doSearch(INDEX, size, queryBuilder, client, mapper, ElasticEdge.class);
    }
}
