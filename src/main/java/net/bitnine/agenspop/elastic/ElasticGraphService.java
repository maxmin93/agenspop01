package net.bitnine.agenspop.elastic;

import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;
import net.bitnine.agenspop.elastic.document.ElasticElementDocument;
import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import net.bitnine.agenspop.elastic.repository.ElasticEdgeRepository;
import net.bitnine.agenspop.elastic.repository.ElasticVertexRepository;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.*;

@Service
public class ElasticGraphService implements ElasticGraphAPI {

    static final String VERTEX_INDEX_NAME = ElasticVertexDocument.class.getAnnotation(Document.class).indexName();
//        "agensvertex";
    static final String EDGE_INDEX_NAME = ElasticEdgeDocument.class.getAnnotation(Document.class).indexName();
//        "agensedge";

    private final ElasticVertexRepository vertexRepository;
    private final ElasticEdgeRepository edgeRepository;
    private final ElasticsearchTemplate template;
    // private final ElasticsearchOperations operations;

    @Autowired
    public ElasticGraphService(
            ElasticVertexRepository vertexRepository,
            ElasticEdgeRepository edgeRepository,
            ElasticsearchTemplate template
            //, ElasticsearchOperations operations
    ) {
        this.vertexRepository = vertexRepository;
        this.edgeRepository = edgeRepository;
        this.template = template;
//        this.operations = operations;
    }

    @PreDestroy
    public void deleteIndices() {
        this.shutdown();
    }

    @PostConstruct
    public void insertDataSample() {
        this.startup();
    }
    /*
    final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29);
    final Vertex vadas = g.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27);
    final Vertex lop = g.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
    final Vertex josh = g.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32);
    final Vertex ripple = g.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
    final Vertex peter = g.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35);
     */

    //////////////////////////////////////////////////
    //
    // common access services about ElasticElementWrapper
    //

    @Override
    public ElasticVertex createVertex(String id, String label){
        ElasticVertexDocument v = new ElasticVertexDocument(id, label);
        return v;
    }
    @Override public ElasticVertex saveVertex(ElasticVertex vertex){
        return vertexRepository.save((ElasticVertexDocument)vertex);
    }
    @Override
    public void deleteVertex(ElasticVertex vertex) {
        if( vertex != null )
            vertexRepository.deleteById(vertex.getId());
    }
    @Override
    public boolean existsVertex(String id){ return vertexRepository.existsById(id); }
    @Override
    public Optional<? extends ElasticVertex> getVertexById(String id){
        return vertexRepository.findById(id);
    }
    @Override
    public Iterable<? extends ElasticVertex> vertices() {
        return vertexRepository.findAll();
    }

    @Override
    public ElasticEdge createEdge(String id, String label, String sid, String tid){
        ElasticEdgeDocument e = new ElasticEdgeDocument(id, label, sid, tid);
        return e;
    }
    @Override public ElasticEdge saveEdge(ElasticEdge edge){
        return edgeRepository.save((ElasticEdgeDocument)edge);
    }
    @Override
    public void deleteEdge(ElasticEdge edge) {
        if( edge != null )
            edgeRepository.deleteById(edge.getId());
    }
    @Override
    public boolean existsEdge(String id){ return edgeRepository.existsById(id); }
    @Override
    public Optional<? extends ElasticEdge> getEdgeById(String id){
        return edgeRepository.findById(id);
    }
    @Override
    public Iterable<? extends ElasticEdge> edges() {
        return edgeRepository.findAll();
    }

    //////////////////////////////////////////////////
    //
    // Admin
    //

    @Override
    public ElasticTx tx(){
        return new ElasticTx() {
            @Override public void failure() {
            }
            @Override public void success() {
            }
            @Override public void close() {
            }
        };
    }

    @Override
    public void shutdown(){
        template.deleteIndex(ElasticVertexDocument.class);
        System.out.println("** Delete index before destruction : "+VERTEX_INDEX_NAME);
        template.deleteIndex(ElasticEdgeDocument.class);
        System.out.println("** Delete index before destruction : "+EDGE_INDEX_NAME);
    }

    @Override
    public void startup(){
        // create index of Vertex documents
        template.createIndex(ElasticVertexDocument.class);
        template.putMapping(ElasticVertexDocument.class);
        template.refresh(ElasticVertexDocument.class);
        System.out.println("** create index of documents : "+VERTEX_INDEX_NAME);

        // create index of Edge documents
        template.createIndex(ElasticEdgeDocument.class);
        template.putMapping(ElasticEdgeDocument.class);
        template.refresh(ElasticEdgeDocument.class);
        System.out.println("** create index of documents : "+EDGE_INDEX_NAME);
    }

    //////////////////////////////////////////////////
    //
    // about Schema
    //

    @Override
    public long countV() {
        return vertexRepository.count();
    }
    @Override
    public long countE() {
        return edgeRepository.count();
    }


    @Override
    public long countV(String datasource){
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices(VERTEX_INDEX_NAME).withTypes(VERTEX_INDEX_NAME) //
                .withSearchType(SearchType.DEFAULT) //
                .withQuery(QueryBuilders
                        .matchQuery("datasource", datasource)
                ).build();
        return template.count(searchQuery);
    }
    @Override
    public long countE(String datasource){
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices(EDGE_INDEX_NAME).withTypes(EDGE_INDEX_NAME) //
                .withSearchType(SearchType.DEFAULT) //
                .withQuery(QueryBuilders
                        .matchQuery("datasource", datasource)
                ).build();
        return template.count(searchQuery);
    }

    @Override
    public boolean hasSchema(String label){
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices(VERTEX_INDEX_NAME).withTypes(VERTEX_INDEX_NAME) //
                .withSearchType(SearchType.DEFAULT) //
                .withQuery(QueryBuilders
                        .matchQuery("label", label)
                ).build();
        long count = template.count(searchQuery);
        return count > 0L;
    }

    @Override
    public boolean hasSchema(String label, String key){
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withIndices(VERTEX_INDEX_NAME).withTypes(VERTEX_INDEX_NAME) //
                .withSearchType(SearchType.DEFAULT) //
                .withQuery(QueryBuilders.boolQuery()
                        .should(matchQuery("label", label))
                        .should(matchQuery("properties.key", key))
                ).build();
        long count = template.count(searchQuery);
        return count > 0L;
    }

    @Override
    public Map<String, Long> listVertexDatasources(){
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(matchAllQuery())
                .withSearchType(SearchType.DEFAULT)
                .withIndices(VERTEX_INDEX_NAME).withTypes(VERTEX_INDEX_NAME)
                .addAggregation(AggregationBuilders.terms("datasources")
                        .field("datasource")
                        .order(BucketOrder.key(true))
                )
                .build();

        Aggregations aggregations = template.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Map<String, Long> result = new HashMap<>();
        Terms labels = aggregations.get("datasources");
        labels.getBuckets().forEach(b->{
            result.put(b.getKeyAsString(), b.getDocCount());
            // for DEBUG
            System.out.println("  - key="+b.getKeyAsString() + ", cnt=" + b.getDocCount());
        });
        return result;
    }

    @Override
    public Map<String, Long> listEdgeDatasources(){
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(matchAllQuery())
                .withSearchType(SearchType.DEFAULT)
                .withIndices(EDGE_INDEX_NAME).withTypes(EDGE_INDEX_NAME)
                .addAggregation(AggregationBuilders.terms("datasources")
                        .field("datasource")
                        .order(BucketOrder.key(true))
                )
                .build();

        Aggregations aggregations = template.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        Map<String, Long> result = new HashMap<>();
        Terms labels = aggregations.get("datasources");
        labels.getBuckets().forEach(b->{
            result.put(b.getKeyAsString(), b.getDocCount());
            // for DEBUG
            System.out.println("  - key="+b.getKeyAsString() + ", cnt=" + b.getDocCount());
        });
        return result;
    }

    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    @Override
    public Iterable<? extends ElasticVertex> findVertices(String datasource){
        return vertexRepository.findByDatasource(datasource);
    }
    @Override
    public Iterable<? extends ElasticVertex> findVertices(String datasource, String label){
        return vertexRepository.findByDatasourceAndLabel(datasource, label);
    }
    @Override
    public Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key){
        return vertexRepository.findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(datasource, label, key);
    }
    @Override
    public Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key, Object value){
        return vertexRepository.findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(
                datasource, label, key, value.toString()
        );
    }

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    @Override
    public Iterable<? extends ElasticEdge> findEdgesBySid(String sid){
        return edgeRepository.findBySid(sid);
    }
    @Override
    public Iterable<? extends ElasticEdge> findEdgesByTid(String tid){
        return edgeRepository.findByTid(tid);
    }
    @Override
    public Iterable<? extends ElasticEdge> findEdgesBySidAndTid(String sid, String tid){
        return edgeRepository.findBySidAndTid(sid, tid);
    }

    @Override
    public Iterable<? extends ElasticEdge> findEdges(String datasource){
        return edgeRepository.findByDatasource(datasource);
    }
    @Override
    public Iterable<? extends ElasticEdge> findEdges(String datasource, String label){
        return edgeRepository.findByDatasourceAndLabel(datasource, label);
    }
    @Override
    public Iterable<? extends ElasticEdge> findEdges(String datasource, String label, String key){
        return edgeRepository.findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(datasource, label, key);
    }
    @Override
    public Iterable<? extends ElasticEdge> findEdges(String datasource, String label, String key, Object value){
        return edgeRepository.findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(datasource, label, key, value.toString());
    }

    //////////////////////////////////////////////////
    //
    // aggregation services of Vertex
    //

    public List<String> verticesAggTest1(){

        // **참고
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_bucket_aggregations.html#_order
        // https://www.baeldung.com/spring-data-elasticsearch-queries
        //
        // **참고: 멀티 필드 그룹핑
        // https://stackoverflow.com/a/42647596/6811653

        // given
        SearchQuery searchQuery = new NativeSearchQueryBuilder() //
                .withQuery(matchAllQuery()) //
                .withSearchType(SearchType.DEFAULT) //
                .withIndices(VERTEX_INDEX_NAME).withTypes(VERTEX_INDEX_NAME) //
                .addAggregation(
                        AggregationBuilders.terms("labels").field("label")
                                .subAggregation(
                                        AggregationBuilders.terms("datasources").field("datasource")
                                )
                )
                .build();
        // when
        Aggregations aggregations = template.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });

        // for DEBUG
        aggregations.asList().stream().forEach(r -> {
            System.out.println("** Aggregation ==> "+r.toString());
        });

        List<String> result = new ArrayList<>();

        // response
        Terms labels = aggregations.get("labels");
        labels.getBuckets().stream().forEach(b1 -> {
            String subKey1 = b1.getKey().toString();
            Long count1 = b1.getDocCount();
            result.add(String.format("  - subKey: %s (=%d)", subKey1, count1));

            Terms datasources = b1.getAggregations().get("datasources");
            datasources.getBuckets().stream().forEach(b2 -> {
                String subKey2 = b2.getKey().toString();
                Long count2 = b2.getDocCount();
                result.add(String.format("    + %s: %d", subKey2, count2));
            });
        });
/*
{
    "labels":{
        "doc_count_error_upper_bound":0,
        "sum_other_doc_count":0,
        "buckets":[
            {
                "key":"person",
                "doc_count":5,
                "datasources":{
                    "doc_count_error_upper_bound":0,
                    "sum_other_doc_count":0,
                    "buckets":[
                        {"key":"default","doc_count":4},
                        {"key":"mysql","doc_count":1}
                    ]
                }
            },{
                "key":"software",
                "doc_count":2,
                "datasources":{
                    "doc_count_error_upper_bound":0,
                    "sum_other_doc_count":0,
                    "buckets":[
                        {"key":"default","doc_count":2}
                    ]
                }
            }
        ]
    }
}
 */

        return result;
    }

    public List<String> verticesAggTest2(){

        // **참고
        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/_bucket_aggregations.html#_order
        // https://www.baeldung.com/spring-data-elasticsearch-queries
        //

        // given
        SearchQuery searchQuery = new NativeSearchQueryBuilder() //
                .withQuery(matchAllQuery()) //
                .withSearchType(SearchType.DEFAULT) //
                .withIndices(VERTEX_INDEX_NAME).withTypes(VERTEX_INDEX_NAME) //
                .addAggregation(AggregationBuilders.terms("labels")
                        .field("label")
                        .order(BucketOrder.key(true))
                )
                .addAggregation(AggregationBuilders.terms("datasources")
                        .field("datasource")
                        .order(BucketOrder.key(true))
                )
                .build();
        // when
        Aggregations aggregations = template.query(searchQuery, new ResultsExtractor<Aggregations>() {
            @Override
            public Aggregations extract(SearchResponse response) {
                return response.getAggregations();
            }
        });
/*
{
    "datasources":{
        "doc_count_error_upper_bound":0,
        "sum_other_doc_count":0,
        "buckets":[
            {"key":"default","doc_count":6},
            {"key":"mysql","doc_count":1}
        ]
    }
},
{
    "labels":{
        "doc_count_error_upper_bound":0,
        "sum_other_doc_count":0,
        "buckets":[
            {"key":"person","doc_count":5},
            {"key":"software","doc_count":2}
        ]
    }
}
 */

        List<String> result = new ArrayList<>();
        // for DEBUG
        aggregations.asList().stream().forEach(r -> {
            result.add(r.toString());
            System.out.println("** Aggregation ==> "+r.toString());
        });

        // https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-search-aggs.html
        Terms labels = aggregations.get("datasources");
        if( labels != null ){
            labels.getBuckets().forEach(b->{
                System.out.println("  - key="+b.getKeyAsString() + ", cnt=" + b.getDocCount());
            });
        }

        return result;
    }
}
