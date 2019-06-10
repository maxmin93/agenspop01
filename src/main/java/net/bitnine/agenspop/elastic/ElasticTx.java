package net.bitnine.agenspop.elastic;

public interface ElasticTx extends AutoCloseable {

    void failure();
    void success();
    void close();

}
