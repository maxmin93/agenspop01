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

    public ElasticVertexDocument(ElasticVertex vertex){
        super(vertex.getEid(), vertex.getLabel(), vertex.getDatasource());
        this.id = vertex.getId();
        this.setProperties(vertex.getProperties());
    }


    @Override
    public String toString() {
        return "ElasticVertexDocument{" +
                " id='" + id + '\'' +
                ", deleted=" + deleted +
                ", eid=" + eid +
                ", label='" + label + '\'' +
                ", datasource='" + datasource + '\'' +
                ", properties=[" + properties.stream().map(ElasticProperty::key).collect(Collectors.joining(",")) +
                "]}";
    }
}
