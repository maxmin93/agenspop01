package net.bitnine.agenspop.elasticgraph.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import net.bitnine.agenspop.basegraph.model.BaseEdge;

@Data
@EqualsAndHashCode(callSuper=false)
public class ElasticEdge extends ElasticElement implements BaseEdge {

    private String sid;     // id of out-vertex : source
    private String tid;     // id of in-vertex : target

    public ElasticEdge(String datasource, String id, String label, String sid, String tid){
        super(datasource, id, label);
        this.sid = sid;
        this.tid = tid;
    }
}
