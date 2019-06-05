package net.bitnine.agenspop.graph.structure.es;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;

public interface ElasticVertexWrapper extends ElasticElementWrapper {

    Set<String> labels();
    boolean hasLabel(String label);
    void addLabel(String label);
    void removeLabel(String label);

    int degree(Direction direction, String type);
    Iterable<ElasticEdgeWrapper> relationships(Direction direction, String...types);
    ElasticEdgeWrapper connectTo(ElasticVertexWrapper node, String type);

}
