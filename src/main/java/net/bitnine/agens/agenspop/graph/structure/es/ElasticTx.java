package net.bitnine.agens.agenspop.graph.structure.es;

public interface ElasticTx extends AutoCloseable{

    void failure();

    void success();
    void close();
}