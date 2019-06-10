package net.bitnine.agenspop.graph.structure.trait;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;

import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.graph.structure.AgensVertexProperty;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

public interface AgensTrait {

    public Predicate<ElasticVertex> getVertexPredicate();

    public Predicate<ElasticEdge> getEdgePredicate();

    public void removeVertex(final AgensVertex vertex);

    public <V> VertexProperty<V> getVertexProperty(final AgensVertex vertex, final String key);

    public <V> Iterator<VertexProperty<V>> getVertexProperties(final AgensVertex vertex, final String... keys);

    public <V> VertexProperty<V> setVertexProperty(final AgensVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues);

    ////

    public boolean supportsMultiProperties();

    public boolean supportsMetaProperties();

    public VertexProperty.Cardinality getCardinality(final String key);

    public void removeVertexProperty(final AgensVertexProperty vertexProperty);

    public <V> Property<V> setProperty(final AgensVertexProperty vertexProperty, final String key, final V value);

    public <V> Property<V> getProperty(final AgensVertexProperty vertexProperty, final String key);

    public <V> Iterator<Property<V>> getProperties(final AgensVertexProperty vertexProperty, final String... keys);

    ////

    public Iterator<Vertex> lookupVertices(final AgensGraph graph, final List<HasContainer> hasContainers, final Object... ids);

}
