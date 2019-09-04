package net.bitnine.agenspop.elasticgraph.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bitnine.agenspop.elasticgraph.model.ElasticElement;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;


// **NOTE: full document request over 10000 records
//
// https://github.com/spring-projects/spring-data-elasticsearch/blob/master/src/main/java/org/springframework/data/elasticsearch/core/ElasticsearchRestTemplate.java
// https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search-scroll.html

public class ElasticScrollIterator<T extends ElasticElement> implements Iterator<List<T>> {

    public static int REQUEST_MAX_SIZE = 10000;
    public static Scroll SCROLL_TIME = new Scroll(TimeValue.timeValueMinutes(1L));

    private final RestHighLevelClient client;
    private final String index;
    private final String datasource;
    private final Class<T> tClass;
    private final ObjectMapper mapper;

    private String scrollId;
    private SearchHit[] searchHits;

    public ElasticScrollIterator(RestHighLevelClient client, String index, String datasource, Class<T> tClass, ObjectMapper mapper){
        this.client = client;
        this.index = index;
        this.datasource = datasource;
        this.tClass = tClass;
        this.mapper = mapper;

        startScroll();
    }

    @Override
    public boolean hasNext() {
        if( searchHits != null && searchHits.length > 0 ) return true;
        // clear scroll
        endScroll();
        return false;
    }

    @Override
    public List<T> next() {
        if( doScroll() ) {
            List<T> documents = new ArrayList<>();
            for (SearchHit hit : searchHits) {
                documents.add(mapper.convertValue(hit.getSourceAsMap(), tClass));
            }
            return documents;
        }
        return Collections.EMPTY_LIST;
    }

    ///////////////////////////////////////////////////////

    private void startScroll() {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.boolQuery()
                    .filter(termQuery("datasource", datasource)));   // All
            searchSourceBuilder.size(REQUEST_MAX_SIZE);                     // LIMIT
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(SCROLL_TIME);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
        }catch (Exception ex){
            searchHits = null;
        }
    }

    private boolean doScroll() {
        try{
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(SCROLL_TIME);
            SearchResponse searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
            return true;
        }catch (Exception ex){
            searchHits = null;
        }
        return false;
    }

    private boolean endScroll() {
        if( scrollId == null ) return false;
        try {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            return clearScrollResponse.isSucceeded();
        }catch (Exception ex){
            searchHits = null;
        }
        return false;
    }

}
