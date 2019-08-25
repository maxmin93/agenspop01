package net.bitnine.agenspop.elasticgraph.model;

import lombok.Data;
import net.bitnine.agenspop.basegraph.model.BaseEdge;

@Data
public class ElasticEdge extends ElasticElement implements BaseEdge {

    private String sid;     // id of out-vertex : source
    private String tid;     // id of in-vertex : target

}
