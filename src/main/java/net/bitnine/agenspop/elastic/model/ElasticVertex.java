package net.bitnine.agenspop.elastic.model;

import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Set;

public interface ElasticVertex extends ElasticElement {

    public static final String DEFAULT_LABEL = "vertex";

    /*
    // **NOTE: Edge index 에서 탐색
    //
    int degree(Direction direction, String type);
    Iterable<ElasticEdge> relationships(Direction direction, String...types);
    ElasticEdge connectTo(ElasticVertex vertex, String label);
    */
}
