package net.bitnine.agenspop.elastic.document;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.stream.Collectors;

// http://localhost:9200/agensvertex/_search?pretty=true&size=20&q=label:orders

@Document(indexName = "agensvertex", type = "agensvertex", shards = 1, replicas = 0,
        refreshInterval = "-1")
public class ElasticVertexDocument extends ElasticElementDocument implements ElasticVertex {

    public ElasticVertexDocument(){
        super();
    }
    public ElasticVertexDocument(String id, String label){
        super(id, label);
    }

    public ElasticVertexDocument(ElasticVertex vertex){
        super(vertex.getId(), vertex.getLabel());
        this.setProperties(vertex.getProperties());
    }

    @Override
    public String toString() {
        return "ElasticVertex{" +
                "deleted=" + deleted +
                ", id='" + id + '\'' +
                ", label='" + label + '\'' +
                ", properties=[" + properties.stream().map(ElasticProperty::getKey).collect(Collectors.joining(",")) +
                "]}";
    }
}
