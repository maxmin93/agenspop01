package net.bitnine.agenspop.elastic;

import com.google.common.collect.Iterables;
import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;
import net.bitnine.agenspop.elastic.document.ElasticElementDocument;
import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import net.bitnine.agenspop.elastic.repository.ElasticEdgeRepository;
import net.bitnine.agenspop.elastic.repository.ElasticVertexRepository;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.DeleteQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.query.QueryBuilders.*;

// http://localhost:9200/_cat/indices?v
// curl -X DELETE "localhost:9200/article?pretty"
// curl -X GET "localhost:9200/_cat/indices?v"

@Service
public class ElasticGraphService implements ElasticGraphAPI {

    static final String VERTEX_INDEX_NAME = ElasticVertexDocument.class.getAnnotation(Document.class).indexName();
//        "agensvertex";
    static final String EDGE_INDEX_NAME = ElasticEdgeDocument.class.getAnnotation(Document.class).indexName();
//        "agensedge";

    // default pageable
    static final Pageable DEFAULT_PAGEABLE = PageRequest.of(0, 2500);

    private final ElasticVertexRepository vertexRepository;
    private final ElasticEdgeRepository edgeRepository;
    private final ElasticsearchTemplate template;
    // private final ElasticsearchOperations operations;

    @Autowired
    public ElasticGraphService(
            ElasticVertexRepository vertexRepository,
            ElasticEdgeRepository edgeRepository,
            ElasticsearchTemplate template,
            TransportClient client
            //, ElasticsearchOperations operations
    ) {
        this.vertexRepository = vertexRepository;
        this.edgeRepository = edgeRepository;
        this.template = template;
//        this.operations = operations;
    }

//    @PreDestroy
//    public void deleteIndices() {
//        this.shutdown();
//    }

    @PostConstruct
    public void insertDataSample() {
        // this.shutdown();
        this.startup();

        ////////////////////////////////
        String datasource = "modern";

        // test : vertices
        List<ElasticVertexDocument> vertices;
        System.out.println("=================================================================");

        List<String> labels = new ArrayList<>(Arrays.asList("person", "software"));
        List<String> keys = new ArrayList<>(Arrays.asList("age", "name"));
        List<String> valuesList = new ArrayList<>(Arrays.asList("35", "marko"));

        // case: ~label.eq
        // AND key.eq
        vertices = vertexRepository.findByDatasourceAndLabelAndPropertiesKey(
                datasource, labels.get(0), keys.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test01] Vertices=%d : ~label.eq='%s' and key.eq='%s'", vertices.size(), labels.get(0), keys.get(0)));

        vertices = vertexRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValue(
                datasource, labels.get(0), keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test02] Vertices=%d : ~label.eq='%s' and key.eq='%s' and value.eq='%s'", vertices.size(), labels.get(0), keys.get(0), valuesList.get(0)));

        vertices = vertexRepository.findByDatasourceAndLabelAndPropertiesKeyIn(
                datasource, labels.get(0), keys, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test03] Vertices=%d : ~label.eq='%s' and keys.in=[%s]", vertices.size(), labels.get(0), String.join("&", keys) ));

        vertices = vertexRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValueIn(
                datasource, labels.get(0), keys.get(0), valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test04] Vertices=%d : ~label.eq='%s' and key.eq='%s' and value.in=[%s]", vertices.size(), labels.get(0), keys.get(0), String.join("&", valuesList)));

        vertices = vertexRepository.findByDatasourceAndLabel(datasource, labels.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test05] Vertices=%d : ~label.eq='%s'", vertices.size(), labels.get(0)));

        vertices = vertexRepository.findByDatasourceAndPropertiesKey(
                datasource, keys.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test06] Vertices=%d : key.eq='%s'", vertices.size(), keys.get(0)));

        vertices = vertexRepository.findByDatasourceAndPropertiesKeyAndPropertiesValue(
                datasource, keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test07] Vertices=%d : key.eq='%s' and value.eq='%s'", vertices.size(), keys.get(0), valuesList.get(0)));

        vertices = vertexRepository.findByDatasourceAndPropertiesKeyIn(
                datasource, keys, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test08] Vertices=%d : key.in=[%s]", vertices.size(), String.join("&", keys)));

        vertices = vertexRepository.findByDatasourceAndPropertiesValueIn(
                datasource, valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test09] Vertices=%d : value.in=[%s]", vertices.size(), String.join("&", valuesList)));

        vertices = vertexRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueIn(
                datasource, keys.get(0), valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test10] Vertices=%d : key.eq='%s' and value.in=[%s]", vertices.size(), keys.get(0), String.join("&", valuesList)));

        vertices = vertexRepository.findByDatasourceAndLabelIn(datasource, labels, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test11] Vertices=%d : ~label.in=[%s]", vertices.size(), String.join("&", labels)));

        vertices = vertexRepository.findByDatasourceAndPropertiesKeyAndLabelIn(
                datasource, keys.get(0), labels, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test12] Vertices=%d : ~label.in=[%s] and key.eq='%s'", vertices.size(), String.join("&", labels), keys.get(0)));

        vertices = vertexRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueAndLabelIn(
                datasource, keys.get(0), valuesList.get(0), labels, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test13] Vertices=%d : ~label.in=[%s] and key.eq='%s' and value.eq='%s'", vertices.size(), String.join("&", labels), keys.get(0), valuesList.get(0)));

        vertices = vertexRepository.findByDatasourceAndLabelInAndPropertiesKeyIn(
                datasource, labels, keys, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test14] Vertices=%d : ~label.in=[%s] and key.in=[%s]", vertices.size(), String.join("&", labels), String.join("&", keys)));

        vertices = vertexRepository.findByDatasourceAndLabelInAndPropertiesValueIn(
                datasource, labels, valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test15] Vertices=%d : ~label.in=[%s] and value.in=[%s]", vertices.size(), String.join("&", labels), String.join("&", valuesList)));

        vertices = vertexRepository.findByDatasourceAndPropertiesKeyAndLabelInAndPropertiesValueIn(
                datasource, keys.get(0), labels, valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test16] Vertices=%d : ~label.in=[%s] and key.eq='%s' and value.in=[%s]", vertices.size(), String.join("&", labels), keys.get(0), String.join("&", valuesList)));

        // test : edges
        List<ElasticEdgeDocument> edges;
        System.out.println("=================================================================");

        labels = new ArrayList<>(Arrays.asList("knows", "created"));
        keys = new ArrayList<>(Arrays.asList("weight", "name"));
        valuesList = new ArrayList<>(Arrays.asList("0.5", "marko"));

        // case: ~label.eq
        // AND key.eq
        edges = edgeRepository.findByDatasourceAndLabelAndPropertiesKey(
                datasource, labels.get(0), keys.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test01] Edges=%d : ~label.eq='%s' and key.eq='%s'", edges.size(), labels.get(0), keys.get(0)));

        edges = edgeRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValue(
                datasource, labels.get(0), keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test02] Edges=%d : ~label.eq='%s' and key.eq='%s' and value.eq='%s'", edges.size(), labels.get(0), keys.get(0), valuesList.get(0)));

        edges = edgeRepository.findByDatasourceAndLabelAndPropertiesKeyIn(
                datasource, labels.get(0), keys, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test03] Edges=%d : ~label.eq='%s' and keys.in=[%s]", edges.size(), labels.get(0), String.join("&", keys) ));

        edges = edgeRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValueIn(
                datasource, labels.get(0), keys.get(0), valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test04] Edges=%d : ~label.eq='%s' and key.eq='%s' and value.in=[%s]", edges.size(), labels.get(0), keys.get(0), String.join("&", valuesList)));

        edges = edgeRepository.findByDatasourceAndLabel(datasource, labels.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test05] Edges=%d : ~label.eq='%s'", edges.size(), labels.get(0)));

        edges = edgeRepository.findByDatasourceAndPropertiesKey(
                datasource, keys.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test06] Edges=%d : key.eq='%s'", edges.size(), keys.get(0)));

        edges = edgeRepository.findByDatasourceAndPropertiesKeyAndPropertiesValue(
                datasource, keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
        System.out.println(String.format("[test07] Edges=%d : key.eq='%s' and value.eq='%s'", edges.size(), keys.get(0), valuesList.get(0)));

        edges = edgeRepository.findByDatasourceAndPropertiesKeyIn(
                datasource, keys, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test08] Edges=%d : key.in=[%s]", edges.size(), String.join("&", keys)));

        edges = edgeRepository.findByDatasourceAndPropertiesValueIn(
                datasource, valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test09] Edges=%d : value.in=[%s]", edges.size(), String.join("&", valuesList)));

        edges = edgeRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueIn(
                datasource, keys.get(0), valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test10] Edges=%d : key.eq='%s' and value.in=[%s]", edges.size(), keys.get(0), String.join("&", valuesList)));

        edges = edgeRepository.findByDatasourceAndLabelIn(datasource, labels, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test11] Edges=%d : ~label.in=[%s]", edges.size(), String.join("&", labels)));

        edges = edgeRepository.findByDatasourceAndPropertiesKeyAndLabelIn(
                datasource, keys.get(0), labels, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test12] Edges=%d : ~label.in=[%s] and key.eq='%s'", edges.size(), String.join("&", labels), keys.get(0)));

        edges = edgeRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueAndLabelIn(
                datasource, keys.get(0), valuesList.get(0), labels, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test13] Edges=%d : ~label.in=[%s] and key.eq='%s' and value.eq='%s'", edges.size(), String.join("&", labels), keys.get(0), valuesList.get(0)));

        edges = edgeRepository.findByDatasourceAndLabelInAndPropertiesKeyIn(
                datasource, labels, keys, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test14] Edges=%d : ~label.in=[%s] and key.in=[%s]", edges.size(), String.join("&", labels), String.join("&", keys)));

        edges = edgeRepository.findByDatasourceAndLabelInAndPropertiesValueIn(
                datasource, labels, valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test15] Edges=%d : ~label.in=[%s] and value.in=[%s]", edges.size(), String.join("&", labels), String.join("&", valuesList)));

        edges = edgeRepository.findByDatasourceAndPropertiesKeyAndLabelInAndPropertiesValueIn(
                datasource, keys.get(0), labels, valuesList, DEFAULT_PAGEABLE);
        System.out.println(String.format("[test16] Edges=%d : ~label.in=[%s] and key.eq='%s' and value.in=[%s]", edges.size(), String.join("&", labels), keys.get(0), String.join("&", valuesList)));

        System.out.println("=================================================================\n");
    }

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

    @Override
    public boolean removeDatasource(String datasource){
        // set target datasource
        DeleteQuery deleteQuery = new DeleteQuery();
        deleteQuery.setQuery(termQuery("datasource", datasource));
        // delete vertices
        template.delete(deleteQuery, ElasticVertexDocument.class);
        template.refresh(ElasticVertexDocument.class);
        // delete edges
        template.delete(deleteQuery, ElasticEdgeDocument.class);
        template.refresh(ElasticEdgeDocument.class);
        // verify
        return (countV(datasource) + countE(datasource)) == 0L;
    }

    //////////////////////////////////////////////////
    //
    // Bulk Insert/Update (SaveAll)
    //
    // ** 참고
    // https://docs.spring.io/spring-data/elasticsearch/docs/current/api/
    //
    // <S extends T> Iterable<S> saveAll(Iterable<S> entities)

    public Iterable<ElasticVertexDocument> bulkInsertV(Iterable<ElasticVertexDocument> vlist){
        return vertexRepository.saveAll(vlist);
    }

    public Iterable<ElasticEdgeDocument> bulkInsertE(Iterable<ElasticEdgeDocument> elist){
        return edgeRepository.saveAll(elist);
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
        if( template.indexExists(ElasticVertexDocument.class) ) {
            template.deleteIndex(ElasticVertexDocument.class);
            System.out.println("** Delete index before destruction : " + VERTEX_INDEX_NAME);
        }
        if( template.indexExists(ElasticEdgeDocument.class) ) {
            template.deleteIndex(ElasticEdgeDocument.class);
            System.out.println("** Delete index before destruction : " + EDGE_INDEX_NAME);
        }
    }

    @Override
    public void startup(){
        if( !template.indexExists(ElasticVertexDocument.class) ){
            // create index of Vertex documents
            template.createIndex(ElasticVertexDocument.class);
            template.putMapping(ElasticVertexDocument.class);
            template.refresh(ElasticVertexDocument.class);
            System.out.println("** create index of documents : "+VERTEX_INDEX_NAME);
        }

        if( !template.indexExists(ElasticEdgeDocument.class) ) {
            // create index of Edge documents
            template.createIndex(ElasticEdgeDocument.class);
            template.putMapping(ElasticEdgeDocument.class);
            template.refresh(ElasticEdgeDocument.class);
            System.out.println("** create index of documents : " + EDGE_INDEX_NAME);
        }
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
        });
        return result;
    }

    //////////////////////////////////////////////////
    //
    // access services of Vertex
    //

    @Override
    public Iterable<ElasticVertex> findVertices(final String... ids){
        if( ids == null || ids.length == 0 ) return Collections.EMPTY_LIST;
        List<? extends ElasticVertex> list = vertexRepository.findByIdIn(Arrays.asList(ids));
        return (List<ElasticVertex>)list;
    }
    @Override
    public Iterable<ElasticVertex> findVertices(String datasource){
        // label 별로 골고루 DEFAULT_PAGEABLE 만큼 가져오기
        List<? extends ElasticVertex> list = vertexRepository.findByDatasource(datasource, DEFAULT_PAGEABLE);
        return (List<ElasticVertex>)list;
    }
    @Override
    public Iterable<ElasticVertex> findVertices(String datasource, String label){
        // label 별로 골고루 DEFAULT_PAGEABLE 만큼 가져오기
        List<? extends ElasticVertex> list = vertexRepository.findByDatasourceAndLabel(datasource, label, DEFAULT_PAGEABLE);
        return (List<ElasticVertex>)list;
    }
    @Override
    public Iterable<ElasticVertex> findVertices(String datasource, final String... ids){
        if( ids == null || ids.length == 0 ) return findVertices(datasource);
        List<? extends ElasticVertex> list = vertexRepository.findByDatasourceAndIdIn(datasource, Arrays.asList(ids));
        return (List<ElasticVertex>)list;
    }

    @Override
    public Iterable<ElasticVertex> findVertices(String datasource
            , List<String> labels, List<String> keys, List<Object> values){
        List<? extends ElasticVertex> list = null;
        List<String> valuesList = values.stream().map(Object::toString).collect(Collectors.toList());

        // case: ~label.eq
        if( labels.size() == 1 ){
            // AND key.eq
            if( keys.size() == 1 && values.size() == 0 ){
                list = vertexRepository.findByDatasourceAndLabelAndPropertiesKey(
                        datasource, labels.get(0), keys.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.eq='%s' and key.eq='%s'", list.size(), labels.get(0), keys.get(0)));
            }
            // AND key.eq AND value.eq
            else if( keys.size() == 1 && values.size() == 1 ) {
                list = vertexRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValue(
                        datasource, labels.get(0), keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.eq='%s' and key.eq='%s' and value.eq='%s'", list.size(), labels.get(0), keys.get(0), valuesList.get(0)));
            }
            // AND key.within
            else if( keys.size() > 1 && values.size() == 0 ) {
                list = vertexRepository.findByDatasourceAndLabelAndPropertiesKeyIn(
                        datasource, labels.get(0), keys, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.eq='%s' and keys.in=[%s]", list.size(), labels.get(0), String.join("&", keys) ));
            }
            // AND key.eq AND values.within
            else if( keys.size() == 1 && values.size() > 1 ) {
                list = vertexRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValueIn(
                        datasource, labels.get(0), keys.get(0), valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.eq='%s' and key.eq='%s' and value.in=[%s]", list.size(), labels.get(0), keys.get(0), String.join("&", valuesList)));
            }
            // AND none
            else { // if( keys.size() == 0 && values.size() == 0 )
                list = vertexRepository.findByDatasourceAndLabel(datasource, labels.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.eq='%s'", list.size(), labels.get(0)));
            }
        }
        // case: none
        else if( labels.size() == 0 ){
            // key.eq
            if( keys.size() == 1 && values.size() == 0 ) {
                list = vertexRepository.findByDatasourceAndPropertiesKey(
                        datasource, keys.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : key.eq='%s'", list.size(), keys.get(0)));
            }
            // key.eq AND value.eq
            else if( keys.size() == 1 && values.size() == 1 ) {
                list = vertexRepository.findByDatasourceAndPropertiesKeyAndPropertiesValue(
                        datasource, keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : key.eq='%s' and value.eq='%s'", list.size(), keys.get(0), valuesList.get(0)));
            }
            // key.within
            else if( keys.size() > 1 && values.size() == 0 ) {
                list = vertexRepository.findByDatasourceAndPropertiesKeyIn(
                        datasource, keys, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : key.in=[%s]", list.size(), String.join("&", keys)));
            }
            // value.within
            else if( keys.size() == 0 && values.size() > 1 ) {
                list = vertexRepository.findByDatasourceAndPropertiesValueIn(
                        datasource, valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : value.in=[%s]", list.size(), String.join("&", valuesList)));
            }
            // key.eq AND values.within
            else if( keys.size() == 1 && values.size() > 1 ) {
                list = vertexRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueIn(
                        datasource, keys.get(0), valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : key.eq='%s' and value.in=[%s]", list.size(), keys.get(0), String.join("&", valuesList)));
            }
        }
        // case: ~label.within
        else if( labels.size() > 1 ){
            // AND none
            if( keys.size() == 0 && values.size() == 0 ) {
                list = vertexRepository.findByDatasourceAndLabelIn(datasource, labels, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.in=[%s]", list.size(), String.join("&", labels)));
            }
            // AND key.eq
            else if( keys.size() == 1 && values.size() == 0 ) {
                list = vertexRepository.findByDatasourceAndPropertiesKeyAndLabelIn(
                        datasource, keys.get(0), labels, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.in=[%s] and key.eq='%s'", list.size(), String.join("&", labels), keys.get(0)));
            }
            // AND key.eq AND value.eq
            else if( keys.size() == 1 && values.size() == 1 ) {
                list = vertexRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueAndLabelIn(
                        datasource, keys.get(0), valuesList.get(0), labels, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.in=[%s] and key.eq='%s' and value.eq='%s'", list.size(), String.join("&", labels), keys.get(0), valuesList.get(0)));
            }
            // AND key.within
            else if( keys.size() > 1 && values.size() == 0 ) {
                list = vertexRepository.findByDatasourceAndLabelInAndPropertiesKeyIn(
                        datasource, labels, keys, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.in=[%s] and key.in=[%s]", list.size(), String.join("&", labels), String.join("&", keys)));
            }
            // AND values.within
            else if( keys.size() == 0 && values.size() > 1 ) {
                list = vertexRepository.findByDatasourceAndLabelInAndPropertiesValueIn(
                        datasource, labels, valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.in=[%s] and value.in=[%s]", list.size(), String.join("&", labels), String.join("&", valuesList)));
            }
            // AND key.eq AND values.within
            else if( keys.size() == 1 && values.size() > 1 ) {
                list = vertexRepository.findByDatasourceAndPropertiesKeyAndLabelInAndPropertiesValueIn(
                        datasource, keys.get(0), labels, valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Vertices=%d : ~label.in=[%s] and key.eq='%s' and value.in=[%s]", list.size(), String.join("&", labels), keys.get(0), String.join("&", valuesList)));
            }
        }

        if( list == null ) list = Collections.EMPTY_LIST;
        return (Iterable<ElasticVertex>)list;
    }

    @Override
    public ElasticVertex findOtherVertexOfEdge(String eid, String vid){
        Optional<? extends ElasticEdge> edge = edgeRepository.findById(eid);
        if( edge.isPresent() ){
            String otherId = edge.get().getSid().equals(vid) ? edge.get().getTid() : edge.get().getSid();
            Optional<? extends ElasticVertex> other = vertexRepository.findById(otherId);
            if( other.isPresent() ) return (ElasticVertex)other.get();
        }
        return null;
    }

    @Override
    public Iterable<ElasticVertex> findNeighborVertices(String id, Direction direction, final String... labels){
        final Iterable<? extends ElasticEdge> edges = findEdgesOfVertex(id, direction, labels);
        List<String> vids = StreamSupport.stream(edges.spliterator(),false)
                .map(s->Arrays.asList(s.getSid(),s.getTid()))
                .flatMap(List::stream).distinct()
                .filter(r -> !r.equals(id))
                .collect(Collectors.toList());
        return vids.size() > 0 ? findVertices( vids.toArray(new String[vids.size()]) )
                    : Collections.EMPTY_LIST;
    }

    //////////////////////////////////////////////////
    //
    // access services of Edge
    //

    @Override
    public Iterable<ElasticEdge> findEdgesOfVertex(String id, Direction direction, final String... labels) {
        final Iterable<? extends ElasticEdge> list;
        if( direction.equals(Direction.OUT) ){       // source vertex of edge
            list = ( labels.length == 0 ) ? edgeRepository.findBySid(id, DEFAULT_PAGEABLE)
                    : edgeRepository.findBySidAndLabelIn(id, Arrays.asList(labels), DEFAULT_PAGEABLE);
        }
        else if ( direction.equals(Direction.IN) ) {  // target vertex of edge
            list = ( labels.length == 0 ) ? edgeRepository.findByTid(id, DEFAULT_PAGEABLE)
                    : edgeRepository.findByTidAndLabelIn(id, Arrays.asList(labels), DEFAULT_PAGEABLE);
        }
        else{
            list = ( labels.length == 0 ) ?
                    Sets.newHashSet( Iterables.concat(
                        edgeRepository.findBySid(id, DEFAULT_PAGEABLE), edgeRepository.findByTid(id, DEFAULT_PAGEABLE)
                    ))
                    : Sets.newHashSet( Iterables.concat(
                        edgeRepository.findBySidAndLabelIn(id, Arrays.asList(labels), DEFAULT_PAGEABLE),
                        edgeRepository.findBySidAndLabelIn(id, Arrays.asList(labels), DEFAULT_PAGEABLE)
                    ));
        }
        return (Iterable<ElasticEdge>) list;
    }

    @Override
    public Iterable<ElasticEdge> findEdgesOfVertex(String id, Direction direction, String label, String key, Object value){
        final Iterable<? extends ElasticEdge> list;
        if( direction.equals(Direction.OUT) ){       // source vertex of edge
            list = edgeRepository.findBySidAndLabelAndPropertiesKeyAndPropertiesValue(id, label, key, value.toString(), DEFAULT_PAGEABLE);
        }
        else if ( direction.equals(Direction.IN) ) {  // target vertex of edge
            list = edgeRepository.findByTidAndLabelAndPropertiesKeyAndPropertiesValue(id, label, key, value.toString(), DEFAULT_PAGEABLE);
        }
        else{
            list = Sets.newHashSet( Iterables.concat(
                    edgeRepository.findBySidAndLabelAndPropertiesKeyAndPropertiesValue(id, label, key, value.toString(), DEFAULT_PAGEABLE),
                    edgeRepository.findByTidAndLabelAndPropertiesKeyAndPropertiesValue(id, label, key, value.toString(), DEFAULT_PAGEABLE)
            ));
        }
        return (Iterable<ElasticEdge>) list;
    }

    @Override
    public Iterable<ElasticEdge> findEdgesBySid(String sid){
        final Iterable<? extends ElasticEdge> list = edgeRepository.findBySid(sid, DEFAULT_PAGEABLE);
        return (Iterable<ElasticEdge>) list;
    }
    @Override
    public Iterable<ElasticEdge> findEdgesByTid(String tid){
        final Iterable<? extends ElasticEdge> list = edgeRepository.findByTid(tid, DEFAULT_PAGEABLE);
        return (Iterable<ElasticEdge>) list;
    }
    @Override
    public Iterable<ElasticEdge> findEdgesBySidAndTid(String sid, String tid){
        final Iterable<? extends ElasticEdge> list = edgeRepository.findBySidAndTid(sid, tid, DEFAULT_PAGEABLE);
        return (Iterable<ElasticEdge>) list;
    }

    @Override
    public Iterable<ElasticEdge> findEdges(final String... ids){
        final Iterable<? extends ElasticEdge> list = edgeRepository.findByIdIn(Arrays.asList(ids));
        return (Iterable<ElasticEdge>) list;
    }
    @Override
    public Iterable<ElasticEdge> findEdges(String datasource, final String... ids){
        final Iterable<? extends ElasticEdge> list = edgeRepository.findByDatasourceAndIdIn(datasource, Arrays.asList(ids));
        return (Iterable<ElasticEdge>) list;
    }
    @Override
    public Iterable<ElasticEdge> findEdges(String datasource){
        final Iterable<? extends ElasticEdge> list = edgeRepository.findByDatasource(datasource, DEFAULT_PAGEABLE);
        return (Iterable<ElasticEdge>) list;
    }
    @Override
    public Iterable<ElasticEdge> findEdges(String datasource, String label){
        final Iterable<? extends ElasticEdge> list = edgeRepository.findByDatasourceAndLabel(datasource, label, DEFAULT_PAGEABLE);
        return (Iterable<ElasticEdge>) list;
    }

    @Override
    public Iterable<ElasticEdge> findEdges(String datasource
            , List<String> labels, List<String> keys, List<Object> values){
        List<? extends ElasticEdge> list = null;
        List<String> valuesList = values.stream().map(Object::toString).collect(Collectors.toList());

        // case: ~label.eq
        if( labels.size() == 1 ){
            // AND key.eq
            if( keys.size() == 1 && values.size() == 0 ){
                list = edgeRepository.findByDatasourceAndLabelAndPropertiesKey(
                        datasource, labels.get(0), keys.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.eq='%s' and key.eq='%s'", list.size(), labels.get(0), keys.get(0)));
            }
            // AND key.eq AND value.eq
            else if( keys.size() == 1 && values.size() == 1 ) {
                list = edgeRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValue(
                        datasource, labels.get(0), keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.eq='%s' and key.eq='%s' and value.eq='%s'", list.size(), labels.get(0), keys.get(0), valuesList.get(0)));
            }
            // AND key.within
            else if( keys.size() > 1 && values.size() == 0 ) {
                list = edgeRepository.findByDatasourceAndLabelAndPropertiesKeyIn(
                        datasource, labels.get(0), keys, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.eq='%s' and keys.in=[%s]", list.size(), labels.get(0), String.join("&", keys) ));
            }
            // AND key.eq AND values.within
            else if( keys.size() == 1 && values.size() > 1 ) {
                list = edgeRepository.findByDatasourceAndLabelAndPropertiesKeyAndPropertiesValueIn(
                        datasource, labels.get(0), keys.get(0), valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.eq='%s' and key.eq='%s' and value.in=[%s]", list.size(), labels.get(0), keys.get(0), String.join("&", valuesList)));
            }
            // AND none
            else { // if( keys.size() == 0 && values.size() == 0 )
                list = edgeRepository.findByDatasourceAndLabel(datasource, labels.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.eq='%s'", list.size(), labels.get(0)));
            }
        }
        // case: none
        else if( labels.size() == 0 ){
            // key.eq
            if( keys.size() == 1 && values.size() == 0 ) {
                list = edgeRepository.findByDatasourceAndPropertiesKey(
                        datasource, keys.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : key.eq='%s'", list.size(), keys.get(0)));
            }
            // key.eq AND value.eq
            else if( keys.size() == 1 && values.size() == 1 ) {
                list = edgeRepository.findByDatasourceAndPropertiesKeyAndPropertiesValue(
                        datasource, keys.get(0), valuesList.get(0), DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : key.eq='%s' and value.eq='%s'", list.size(), keys.get(0), valuesList.get(0)));
            }
            // key.within
            else if( keys.size() > 1 && values.size() == 0 ) {
                list = edgeRepository.findByDatasourceAndPropertiesKeyIn(
                        datasource, keys, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : key.in=[%s]", list.size(), String.join("&", keys)));
            }
            // value.within
            else if( keys.size() == 0 && values.size() > 1 ) {
                list = edgeRepository.findByDatasourceAndPropertiesValueIn(
                        datasource, valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : value.in=[%s]", list.size(), String.join("&", valuesList)));
            }
            // key.eq AND values.within
            else if( keys.size() == 1 && values.size() > 1 ) {
                list = edgeRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueIn(
                        datasource, keys.get(0), valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : key.eq='%s' and value.in=[%s]", list.size(), keys.get(0), String.join("&", valuesList)));
            }
        }
        // case: ~label.within
        else if( labels.size() > 1 ){
            // AND none
            if( keys.size() == 0 && values.size() == 0 ) {
                list = edgeRepository.findByDatasourceAndLabelIn(datasource, labels, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.in=[%s]", list.size(), String.join("&", labels)));
            }
            // AND key.eq
            else if( keys.size() == 1 && values.size() == 0 ) {
                list = edgeRepository.findByDatasourceAndPropertiesKeyAndLabelIn(
                        datasource, keys.get(0), labels, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.in=[%s] and key.eq='%s'", list.size(), String.join("&", labels), keys.get(0)));
            }
            // AND key.eq AND value.eq
            else if( keys.size() == 1 && values.size() == 1 ) {
                list = edgeRepository.findByDatasourceAndPropertiesKeyAndPropertiesValueAndLabelIn(
                        datasource, keys.get(0), valuesList.get(0), labels, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.in=[%s] and key.eq='%s' and value.eq='%s'", list.size(), String.join("&", labels), keys.get(0), valuesList.get(0)));
            }
            // AND key.within
            else if( keys.size() > 1 && values.size() == 0 ) {
                list = edgeRepository.findByDatasourceAndLabelInAndPropertiesKeyIn(
                        datasource, labels, keys, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.in=[%s] and key.in=[%s]", list.size(), String.join("&", labels), String.join("&", keys)));
            }
            // AND values.within
            else if( keys.size() == 0 && values.size() > 1 ) {
                list = edgeRepository.findByDatasourceAndLabelInAndPropertiesValueIn(
                        datasource, labels, valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.in=[%s] and value.in=[%s]", list.size(), String.join("&", labels), String.join("&", valuesList)));
            }
            // AND key.eq AND values.within
            else if( keys.size() == 1 && values.size() > 1 ) {
                list = edgeRepository.findByDatasourceAndPropertiesKeyAndLabelInAndPropertiesValueIn(
                        datasource, keys.get(0), labels, valuesList, DEFAULT_PAGEABLE);
                System.out.println(String.format("** optimized Edges=%d : ~label.in=[%s] and key.eq='%s' and value.in=[%s]", list.size(), String.join("&", labels), keys.get(0), String.join("&", valuesList)));
            }
        }

        if( list == null ) list = Collections.EMPTY_LIST;
        return (Iterable<ElasticEdge>)list;
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
