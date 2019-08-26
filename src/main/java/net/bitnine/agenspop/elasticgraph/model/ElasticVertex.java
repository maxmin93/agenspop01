package net.bitnine.agenspop.elasticgraph.model;

import lombok.Data;
import net.bitnine.agenspop.basegraph.model.BaseVertex;

@Data
public class ElasticVertex extends ElasticElement implements BaseVertex {

    public ElasticVertex(String datasource, String id, String label) {
        super(datasource, id, label);
    }

}
