package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.stream.Collectors;

// http://localhost:9200/agensedge/_search?pretty=true&size=20&q=datasource:modern

@Document(indexName = "agensedge", type = "agensedge"
        , shards = 1, replicas = 0, refreshInterval = "-1")
public class ElasticEdgeDocument extends ElasticElementDocument implements ElasticEdge {

    @Field(type = FieldType.Keyword)
    protected String sid;
    @Field(type = FieldType.Keyword)
    protected String tid;

    public ElasticEdgeDocument(){
        super();
    }
    public ElasticEdgeDocument(String id, String label, String sid, String tid){
        super(id, label);
        this.sid = sid;
        this.tid = tid;
    }
    public ElasticEdgeDocument(ElasticEdge edge){
        super(edge.getId(), edge.getLabel());
        this.sid = edge.getSid();
        this.tid = edge.getTid();
        this.setProperties(edge.getProperties());
    }

    @Override public String getSid(){ return sid; }
    @Override public String getTid(){ return tid; }

    @Override
    public String toString() {
        return "ElasticEdge{" +
                " deleted=" + deleted +
                ", id='" + id + '\'' +
                ", label='" + label + '\'' +
                ", sid='" + sid + '\'' +
                ", tid='" + tid + '\'' +
                ", properties=[" + properties.stream().map(ElasticProperty::getKey).collect(Collectors.joining(",")) +
                "]}";
    }
}
