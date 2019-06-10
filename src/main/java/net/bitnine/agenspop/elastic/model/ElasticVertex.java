package net.bitnine.agenspop.elastic.model;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;

public interface ElasticVertex extends ElasticElement {

    public static final String DEFAULT_LABEL = "vertex";

    Set<String> labels();
    boolean hasLabel(String label);
    void addLabel(String label);
    void removeLabel(String label);

    int degree(Direction direction, String type);
    Iterable<ElasticEdge> relationships(Direction direction, String...types);
    ElasticEdge connectTo(ElasticVertex node, String type);
}
