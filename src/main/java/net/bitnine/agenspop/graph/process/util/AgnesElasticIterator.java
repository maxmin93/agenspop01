package net.bitnine.agenspop.graph.process.util;

import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.graph.structure.es.ElasticEdgeWrapper;
import net.bitnine.agenspop.graph.structure.es.ElasticVertexWrapper;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

// 참고 org.apache.tinkerpop.gremlin.neo4j.process.util.Neo4jCypherIterator
//
public final class AgnesElasticIterator<T> implements Iterator<Map<String, T>> {

    final Iterator<Map<String, T>> iterator;
    final AgensGraph graph;

    public AgnesElasticIterator(final Iterator<Map<String, T>> iterator, final AgensGraph graph) {
        this.iterator = iterator;
        this.graph = graph;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Map<String, T> next() {
        return this.iterator.next().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    final T val = entry.getValue();
                    if (ElasticVertexWrapper.class.isAssignableFrom(val.getClass())) {
                        return (T) new AgensVertex((ElasticVertexWrapper)val, this.graph);
                    } else if (ElasticEdgeWrapper.class.isAssignableFrom(val.getClass())) {
                        return (T) new AgensEdge((ElasticEdgeWrapper)val, this.graph);
                    } else {
                        return val;
                    }
                }));
    }
}