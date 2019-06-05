package net.bitnine.agenspop.dto;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.InnerField;
import org.springframework.data.elasticsearch.annotations.MultiField;

@Document(indexName = "article", type = "article", shards = 1, replicas = 0,
        refreshInterval = "-1")
public class Article {

    @Id
    private String id;

    @MultiField(
            mainField = @Field(type = FieldType.Text, fielddata = true),
            otherFields = { @InnerField(suffix = "verbatim", type = FieldType.Keyword) }
    )
    private String title;

    @Field(type = FieldType.Nested, includeInParent = true)
    private List<Author> authors;

    @Field(type = FieldType.Object, includeInParent = true)
    private Map<String, String> tags = new HashMap<>();

/*
    https://www.elastic.co/guide/en/elasticsearch/reference/6.7/mapping-types.html

    ** Complex datatypes
    Array datatype : Array support does not require a dedicated type
    Object datatype : object for single JSON objects
    Nested datatype : nested for arrays of JSON objects
 */

//    @Field(type = FieldType.Keyword)
//    private String[] tags;

    public Article() {
    }

    public Article(String title) {
        this.title = title;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Author> getAuthors() {
        return authors;
    }

    public void setAuthors(List<Author> authors) {
        this.authors = authors;
    }

    public String getTag(String key) {
        return tags.get(key);
    }

    public void setTag(String key, String val) {
        this.tags.put(key, val);
    }

    @Override
    public String toString() {
        return "Article{" + "id='" + id + '\'' + ", title='" + title + '\''
                + ", authors=" + authors + ", tags=["
                + tags.keySet().stream().collect(Collectors.joining(",")) + "]}";
    }
}