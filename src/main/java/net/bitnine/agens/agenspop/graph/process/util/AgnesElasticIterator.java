package net.bitnine.agens.agenspop.graph.process.util;

import net.bitnine.agens.agenspop.graph.structure.AgensEdge;
import net.bitnine.agens.agenspop.graph.structure.AgensGraph;
import net.bitnine.agens.agenspop.graph.structure.AgensVertex;
import net.bitnine.agens.agenspop.graph.structure.es.ElasticEdge;
import net.bitnine.agens.agenspop.graph.structure.es.ElasticVertex;

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
                    if (ElasticVertex.class.isAssignableFrom(val.getClass())) {
                        return (T) new AgensVertex((ElasticVertex)val, this.graph);
                    } else if (ElasticEdge.class.isAssignableFrom(val.getClass())) {
                        return (T) new AgensEdge((ElasticEdge)val, this.graph);
                    } else {
                        return val;
                    }
                }));
    }
}