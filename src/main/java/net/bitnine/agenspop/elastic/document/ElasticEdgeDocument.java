package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.stream.Collectors;

@Document(indexName = "agensedge", type = "agensedge"
        , shards = 1, replicas = 0, refreshInterval = "-1")
public class ElasticEdgeDocument extends ElasticElementDocument implements ElasticEdge {

    @Field(type = FieldType.Long)
    protected Long sid;
    @Field(type = FieldType.Long)
    protected Long tid;

    public ElasticEdgeDocument(){
        super();
    }
    public ElasticEdgeDocument(Long eid, String label, Long sid, Long tid){
        super(eid, label);
        this.sid = sid;
        this.tid = tid;
    }
    public ElasticEdgeDocument(Long eid, String label, String datasource, Long sid, Long tid){
        super(eid, label, datasource);
        this.sid = sid;
        this.tid = tid;
    }
    public ElasticEdgeDocument(ElasticEdge edge){
        super(edge.getEid(), edge.getLabel(), edge.getDatasource());
        this.sid = edge.getSid();
        this.tid = edge.getTid();
        this.id = edge.getId();
        this.setProperties(edge.getProperties());
    }

    @Override public Long getSid(){ return sid; }
    @Override public Long getTid(){ return tid; }

    @Override
    public String toString() {
        return "ElasticEdgeDocument{ " +
                " id='" + id + '\'' +
                ", deleted=" + deleted +
                ", eid=" + eid +
                ", label='" + label + '\'' +
                ", datasource='" + datasource + '\'' +
                ", sid='" + sid + '\'' +
                ", tid='" + tid + '\'' +
                ", properties=[" + properties.stream().map(ElasticProperty::key).collect(Collectors.joining(",")) +
                "]}";
    }

    ////////////////////////////////////

//    @Override public ElasticVertex start(){
//        return ElasticVertexDocument()
//    }
//
//    @Override public ElasticVertex end(){
//
//    }
//
//    @Override public ElasticVertex other(ElasticVertex node){
//
//    }
}
