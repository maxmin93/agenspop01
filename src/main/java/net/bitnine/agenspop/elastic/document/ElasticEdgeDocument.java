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

    @Field(type = FieldType.Integer)
    protected Integer sid;
    @Field(type = FieldType.Integer)
    protected Integer tid;

    public ElasticEdgeDocument(){
        super();
    }
    public ElasticEdgeDocument(Integer eid, String label, Integer sid, Integer tid){
        super(eid, label);
        this.sid = sid;
        this.tid = tid;
    }
    public ElasticEdgeDocument(Integer eid, String label, String datasource, Integer sid, Integer tid){
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

    @Override public Integer getSid(){ return sid; }
    @Override public Integer getTid(){ return tid; }

    @Override
    public String toString() {
        return "ElasticEdge{ " +
                " id='" + id + '\'' +
                ", deleted=" + deleted +
                ", eid=" + eid +
                ", label='" + label + '\'' +
                ", datasource='" + datasource + '\'' +
                ", sid='" + sid + '\'' +
                ", tid='" + tid + '\'' +
                ", properties=[" + properties.stream().map(ElasticProperty::getKey).collect(Collectors.joining(",")) +
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
