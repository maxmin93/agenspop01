package net.bitnine.agens.agenspop.graph.structure.es;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;

public interface ElasticVertex extends ElasticElement {

    Set<String> labels();
    boolean hasLabel(String label);
    void addLabel(String label);
    void removeLabel(String label);

    int degree(Direction direction, String type);
    Iterable<ElasticEdge> relationships(Direction direction, String...types);
    ElasticEdge connectTo(ElasticVertex node, String type);

}
