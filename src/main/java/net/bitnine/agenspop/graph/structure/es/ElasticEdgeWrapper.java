package net.bitnine.agenspop.graph.structure.es;

public interface ElasticEdgeWrapper extends ElasticElementWrapper {

    String type();
    ElasticVertexWrapper start();
    ElasticVertexWrapper end();
    ElasticVertexWrapper other(ElasticVertexWrapper node);
}
