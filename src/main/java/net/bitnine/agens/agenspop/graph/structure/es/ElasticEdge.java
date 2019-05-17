package net.bitnine.agens.agenspop.graph.structure.es;

public interface ElasticEdge extends ElasticElement {

    String type();
    ElasticVertex start();
    ElasticVertex end();
    ElasticVertex other(ElasticVertex node);
}
