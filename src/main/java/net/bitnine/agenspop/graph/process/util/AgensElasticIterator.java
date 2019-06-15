package net.bitnine.agenspop.graph.process.util;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensVertex;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

// 참고 org.apache.tinkerpop.gremlin.neo4j.process.util.Neo4jCypherIterator
//
public final class AgensElasticIterator<T> implements Iterator<Map<String, T>> {

    final Iterator<Map<String, T>> iterator;
    final AgensGraph graph;

    public AgensElasticIterator(final Iterator<Map<String, T>> iterator, final AgensGraph graph) {
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