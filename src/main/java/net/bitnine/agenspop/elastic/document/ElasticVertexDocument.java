package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.stream.Collectors;

@Document(indexName = "elasticvertex", type = "elasticvertex", shards = 1, replicas = 0,
        refreshInterval = "-1")
public class ElasticVertexDocument extends ElasticElementDocument implements ElasticVertex {

    public ElasticVertexDocument(){
        super();
    }
    public ElasticVertexDocument(Long eid, String label){
        super(eid, label);
    }
    public ElasticVertexDocument(Long eid, String label, String datasource){
        super(eid, label, datasource);
    }

/*
    @Override
    public void remove() {
        final List<Edge> edges = new ArrayList<>();
        this.edges(Direction.BOTH).forEachRemaining(edges::add);
        edges.stream().filter(edge -> !((TinkerEdge) edge).removed).forEach(Edge::remove);
        this.properties = null;
        TinkerHelper.removeElementIndex(this);
        this.graph.vertices.remove(this.id);
        this.removed = true;
    }
*/

    @Override
    public String toString() {
        return "ElasticVertexDocument{" +
                " id='" + id + '\'' +
                ", deleted=" + deleted +
                ", eid=" + eid +
                ", label='" + label + '\'' +
                ", datasource='" + datasource + '\'' +
                ", properties=[" + props.stream().map(ElasticProperty::key).collect(Collectors.joining(",")) +
                "]}";
    }
}
