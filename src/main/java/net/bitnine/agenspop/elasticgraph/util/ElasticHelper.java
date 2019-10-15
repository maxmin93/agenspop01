package net.bitnine.agenspop.elasticgraph.util;

import net.bitnine.agenspop.elasticgraph.model.ElasticElement;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

public final class ElasticHelper {

    // **NOTE: Java Exception Handle in Stream Operations
    // https://kihoonkim.github.io/2017/09/09/java/noexception-in-stream-operations/

    public interface ExceptionSupplier<T> {
        T get() throws Exception;
    }

    public static <T> T wrapException(ExceptionSupplier<T> z) {
        try {
            return z.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-search.html
/*

    ** ScoreMode
    total : Add the original score and the rescore query score. The default.
    multiply : Multiply the original score by the rescore query score. Useful for function query rescores.
    avg : Average the original score and the rescore query score.
    max : Take the max of original score and the rescore query score.
    min : Take the min of the original score and the rescore query score.

*/
    public static BoolQueryBuilder addQueryDs(BoolQueryBuilder queryBuilder
            , String datasource){
        return queryBuilder.filter(termQuery("datasource", datasource));
    }

    public static BoolQueryBuilder addQueryLabel(BoolQueryBuilder queryBuilder
            , String label){
        return queryBuilder.filter(termQuery("label", label));
    }

    public static BoolQueryBuilder addQueryLabels(BoolQueryBuilder queryBuilder
            , String[] labels){
        return queryBuilder.filter(termsQuery("label", labels));
    }

    public static BoolQueryBuilder addQueryKey(BoolQueryBuilder queryBuilder, String key){
        return queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                    ), ScoreMode.Total));
    }

    public static BoolQueryBuilder addQueryKeyNot(BoolQueryBuilder queryBuilder, String key){
        return queryBuilder.mustNot(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                    ), ScoreMode.Total));
    }

    public static BoolQueryBuilder addQueryKeys(BoolQueryBuilder queryBuilder, String[] keys){
        for( String key : keys ){       // AND
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            termQuery("properties.key", key)
                    ), ScoreMode.Total));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder addQueryValue(BoolQueryBuilder queryBuilder, String value){
        return queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\"")
                    ), ScoreMode.Total));
    }

    public static BoolQueryBuilder addQueryValues(BoolQueryBuilder queryBuilder, String[] values){
        for( String value : values ){       // AND
            queryBuilder = queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery().must(
                            QueryBuilders.queryStringQuery("properties.value:\"" + value.toLowerCase() + "\"")
                    ), ScoreMode.Total));
        }
        return queryBuilder;
    }

    public static BoolQueryBuilder addQueryKeyValue(BoolQueryBuilder queryBuilder, String key, String value){
        return queryBuilder.must(QueryBuilders.nestedQuery("properties",
                    QueryBuilders.boolQuery()
                        .must(termQuery("properties.key", key.toLowerCase()))
                        .must(QueryBuilders.queryStringQuery("properties.value:\""+value.toLowerCase()+"\""))
                    , ScoreMode.Total));
    }

    public static <T> List<T> postFilterByValue(List<T> list, String filter){
        List<T> temp = new ArrayList<>();
        for( T item : list ){
            List<String> pvalues = ((ElasticElement)item).getProperties().stream().map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            if( pvalues.contains(filter) ) temp.add(item);
        }
        return temp;
    }

    public static <T> List<T> postFilterByValues(List<T> list, List<String> filters){
        List<T> temp = new ArrayList<>();
        for( T item : list ){
            List<String> pvalues = ((ElasticElement)item).getProperties().stream().map(p->p.getValue().toLowerCase()).collect(Collectors.toList());
            if( pvalues.containsAll(filters) ) temp.add(item);
        }
        return temp;
    }
}
