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

    static final String VERTEX_INDEX_NAME = "agensvertex";
    static final String EDGE_INDEX_NAME = "agensedge";

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

        // Save data sample
//        insertMordernVertices();
//        insertMordernEdges();
    }

    /*
            final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29);
            final Vertex vadas = g.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27);
            final Vertex lop = g.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
            final Vertex josh = g.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32);
            final Vertex ripple = g.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
            final Vertex peter = g.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35);
     */
    private void insertMordernVertices(){
        // http://localhost:9200/elasticvertex/_search?pretty=true&q=*:*
        System.out.println("** create ElasticVertexWrapper index : " +
                Arrays.asList(ElasticVertexDocument.class.getAnnotations()).toString());

        ElasticVertexDocument v = new ElasticVertexDocument(1, "person");
        v.setProperty("name", "marko");
        v.setProperty("age", 29);
        ElasticVertexDocument tmp = vertexRepository.save(v);
//        System.out.println("vertex saved: "+tmp.toString());

        v = new ElasticVertexDocument(2, "person");
        v.setProperty("name", "vadas");
        v.setProperty("age", 27);
        vertexRepository.save(v);

        v = new ElasticVertexDocument(3, "software");
        v.setProperty("name", "lop");
        v.setProperty("lang", "java");
        vertexRepository.save(v);

        v = new ElasticVertexDocument(4, "person");
        v.setProperty("name", "josh");
        v.setProperty("age", 32);
        vertexRepository.save(v);

        v = new ElasticVertexDocument(5, "software");
        v.setProperty("name", "ripple");
        v.setProperty("lang", "java");
        vertexRepository.save(v);

        v = new ElasticVertexDocument(6, "person");
        v.setProperty("name", "peter");
        v.setProperty("age", 35);
        vertexRepository.save(v);

        v = new ElasticVertexDocument(6, "person", "mysql");
        v.setProperty("name", "peter1");
        v.setProperty("age", 35);
        vertexRepository.save(v);
    }
    /*
            marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5d);     // 1 -> 2
            marko.addEdge("knows", josh, T.id, 8, "weight", 1.0d);      // 1 -> 4
            marko.addEdge("created", lop, T.id, 9, "weight", 0.4d);     // 1 -> 3
            josh.addEdge("created", ripple, T.id, 10, "weight", 1.0d);  // 4 -> 5
            josh.addEdge("created", lop, T.id, 11, "weight", 0.4d);     // 4 -> 3
            peter.addEdge("created", lop, T.id, 12, "weight", 0.2d);    // 6 -> 3
     */
    private void insertMordernEdges(){
        // http://localhost:9200/elasticedge/_search?pretty=true&q=*:*
        System.out.println("** create ElasticEdgeWrapper index : " +
                Arrays.asList(ElasticEdgeDocument.class.getAnnotations()).toString());

        ElasticEdgeDocument e = new ElasticEdgeDocument(7, "knows", 1, 2);
        e.setProperty("weight", "0.5d");
        ElasticEdgeDocument tmp = edgeRepository.save(e);
//        System.out.println("edge saved: "+tmp.toString());

        e = new ElasticEdgeDocument(8, "knows", 1, 4);
        e.setProperty("weight", "1.0d");
        edgeRepository.save(e);

        e = new ElasticEdgeDocument(9, "created", 1, 3);
        e.setProperty("weight", "0.4d");
        edgeRepository.save(e);

        e = new ElasticEdgeDocument(10, "created", 4, 5);
        e.setProperty("weight", "1.0d");
        edgeRepository.save(e);

        e = new ElasticEdgeDocument(11, "created", 4, 3);
        e.setProperty("weight", "0.4d");
        edgeRepository.save(e);

        e = new ElasticEdgeDocument(12, "created", 6, 3);
        e.setProperty("weight", "0.2d");
        edgeRepository.save(e);
    }

    //////////////////////////////////////////////////

    @Override
    public ElasticVertex createVertex(Integer eid, String label, String datasource){
        ElasticVertexDocument v = new ElasticVertexDocument(eid, label, datasource);
        return v;
    }
    @Override public ElasticVertex saveVertex(ElasticVertex vertex){
        return vertexRepository.save((ElasticVertexDocument)vertex);
    }

    @Override
    public ElasticEdge createEdge(Integer eid, String datasource, String label, Integer sid, Integer tid){
        ElasticEdgeDocument e = new ElasticEdgeDocument(eid, label, datasource, sid, tid);
        return e;
    }
    @Override public ElasticEdge saveEdge(ElasticEdge edge){
        return edgeRepository.save((ElasticEdgeDocument)edge);
    }

    @Override
    public ElasticVertex getVertexById(String datasource, Integer eid){
        List<ElasticVertexDocument> list = vertexRepository.findByEidAndDatasource(eid, datasource);
        return list.size() > 0 ? list.get(0) : null;
    }

    @Override
    public ElasticEdge getEdgeById(String datasource, Integer eid){
        List<ElasticEdgeDocument> list = edgeRepository.findByEidAndDatasource(eid, datasource);
        return list.size() > 0 ? list.get(0) : null;
    }

    @Override
    public void shutdown(){
        template.deleteIndex(ElasticVertexDocument.class);
        System.out.println("** Delete index before destory : "+ElasticVertexDocument.class.getSimpleName());
        template.deleteIndex(ElasticEdgeDocument.class);
        System.out.println("** Delete index before destory : "+ElasticEdgeDocument.class.getSimpleName());
    }

    @Override
    public void startup(){
        // create index of Vertex documents
        template.createIndex(ElasticVertexDocument.class);
        template.putMapping(ElasticVertexDocument.class);
        template.refresh(ElasticVertexDocument.class);
        System.out.println("** create index of Vertex documents : " + ElasticVertexDocument.class.getSimpleName());

        // create index of Edge documents
        template.createIndex(ElasticEdgeDocument.class);
        template.putMapping(ElasticEdgeDocument.class);
        template.refresh(ElasticEdgeDocument.class);
        System.out.println("** create index of Edge documents : " + ElasticEdgeDocument.class.getSimpleName());
    }

    @Override
    public Iterable<? extends ElasticVertex> allVertices(String datasource){
        return vertexRepository.findByDatasource(datasource);
    }

    @Override
    public Iterable<? extends ElasticEdge> allEdges(String datasource){
        return edgeRepository.findByDatasource(datasource);
    }

    @Override
    public Iterable<? extends ElasticVertex> findVertices(String datasource, String label){
        return vertexRepository.findByLabelAndDatasource(label, datasource);
    }
    @Override
    public Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key){
        return vertexRepository.findByDatasourceAndLabelAndPropsKeyUsingCustomQuery(datasource, label, key);
    }
    @Override
    public Iterable<? extends ElasticVertex> findVertices(String datasource, String label, String key, Object value){
        String strValue = value != null ? value.toString() : "";
        return vertexRepository.findByDatasourceAndLabelAndPropsKeyAndValueUsingCustomQuery(datasource, label, key, strValue);
    }

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
    public boolean hasSchemaIndex(String label){
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
    public boolean hasSchemaIndex(String label, String key){
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

    //////////////////////////////////////////////////
    //
    // common access services about ElasticElementWrapper
    //

    @Override
    public long countV() {
        return vertexRepository.count();
    }
    @Override
    public void deleteV(ElasticVertex vertex) {
        if( vertex != null ){
            Optional<ElasticVertexDocument> doc = vertexRepository.findById(vertex.getId());
            if( doc.isPresent() ) vertexRepository.delete(doc.get());
        }
    }
    @Override
    public ElasticVertex saveV(ElasticVertex vertex) {
        if( vertex != null ){
            ElasticVertexDocument doc = new ElasticVertexDocument(vertex);
            vertexRepository.save(doc);
        }
        return (ElasticVertex)vertex;
    }
    @Override
    public Optional<ElasticVertexDocument> vertexOne(String uid) {
        return vertexRepository.findById(uid);
    }
    @Override
    public Iterable<ElasticVertexDocument> vertices() {
        return vertexRepository.findAll();
    }

    @Override
    public long countE() {
        return edgeRepository.count();
    }
    @Override
    public void deleteE(ElasticEdge edge) {
        if( edge != null ){
            Optional<ElasticEdgeDocument> doc = edgeRepository.findById(edge.getId());
            if( doc.isPresent() ) edgeRepository.delete(doc.get());
        }
    }
    @Override
    public ElasticEdge saveE(ElasticEdge edge) {
        if( edge != null ){
            ElasticEdgeDocument doc = new ElasticEdgeDocument(edge);
            edgeRepository.save(doc);
        }
        return (ElasticEdge)edge;
    }
    @Override
    public Optional<ElasticEdgeDocument> edgeOne(String uid) {
        return edgeRepository.findById(uid);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edges() {
        return edgeRepository.findAll();
    }

    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    @Override
    public Optional<ElasticVertexDocument> vertexByEid(Integer eid) {
        List<ElasticVertexDocument> result = vertexRepository.findByEid(eid);
        return result.stream().findFirst();
    }
    @Override
    public Optional<ElasticVertexDocument> vertexByEidAndDatasource(Integer eid, String datasource) {
        List<ElasticVertexDocument> result = vertexRepository.findByEidAndDatasource(eid, datasource);
        return result.stream().findFirst();
    }

    @Override
    public Iterable<ElasticVertexDocument> verticesByLabel(String label) {
        return vertexRepository.findByLabel(label);
    }
    @Override
    public Iterable<ElasticVertexDocument> verticesByLabelAndDatasource(String label, String datasource) {
        return vertexRepository.findByLabelAndDatasource(label, datasource);
    }
    @Override
    public Iterable<ElasticVertexDocument> verticesByPropsKey(String key) {
        return vertexRepository.findByPropsKeyUsingCustomQuery(key);
    }
    @Override
    public Iterable<ElasticVertexDocument> verticesByPropsValue(String value) {
        return vertexRepository.findByPropsValueUsingCustomQuery(value);
    }
    @Override
    public Iterable<ElasticVertexDocument> verticesByPropsKeyAndValue(String key, String value) {
        return vertexRepository.findByPropsKeyAndValueUsingCustomQuery(key, value);
    }

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    @Override
    public Optional<ElasticEdgeDocument> edgeByEid(Integer eid) {
        List<ElasticEdgeDocument> result = edgeRepository.findByEid(eid);
        return result.stream().findFirst();
    }
    @Override
    public Optional<ElasticEdgeDocument> edgeByEidAndDatasource(Integer eid, String datasource) {
        List<ElasticEdgeDocument> result = edgeRepository.findByEidAndDatasource(eid, datasource);
        return result.stream().findFirst();
    }

    @Override
    public Iterable<ElasticEdgeDocument> edgesBySid(Integer sid) {
        return edgeRepository.findBySid(sid);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesBySidAndDatasource(Integer sid, String datasource) {
        return edgeRepository.findBySidAndDatasource(sid, datasource);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesByTid(Integer tid) {
        return edgeRepository.findByTid(tid);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesByTidAndDatasource(Integer tid, String datasource) {
        return edgeRepository.findByTidAndDatasource(tid, datasource);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesBySidAndTid(Integer sid, Integer tid) {
        return edgeRepository.findBySidAndTid(sid, tid);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesBySidAndTidAndDatasource(Integer sid, Integer tid, String datasource) {
        return edgeRepository.findBySidAndTidAndDatasource(sid, tid, datasource);
    }

    @Override
    public Iterable<ElasticEdgeDocument> edgesByLabel(String label) {
        return edgeRepository.findByLabel(label);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesByLabelAndDatasource(String label, String datasource) {
        return edgeRepository.findByLabelAndDatasource(label, datasource);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesByPropsKey(String key) {
        return edgeRepository.findByPropsKeyUsingCustomQuery(key);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesByPropsValue(String value) {
        return edgeRepository.findByPropsValueUsingCustomQuery(value);
    }
    @Override
    public Iterable<ElasticEdgeDocument> edgesByPropsKeyAndValue(String key, String value) {
        return edgeRepository.findByPropsKeyAndValueUsingCustomQuery(key, value);
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
