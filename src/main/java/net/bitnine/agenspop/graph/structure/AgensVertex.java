package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.graph.structure.es.ElasticVertexWrapper;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensVertex extends AgensElement implements Vertex, WrappedVertex<ElasticVertexWrapper> {

    protected Map<String, List<VertexProperty>> properties;
    protected Map<String, Set<Edge>> outEdges;
    protected Map<String, Set<Edge>> inEdges;

    public AgensVertex(final ElasticVertexWrapper vertex, final AgensGraph graph) {
        super(vertex.getId(), vertex.getProperty(T.label.getAccessor()).toString(), graph);
    }

    protected AgensVertex(final Object id, final String label, final AgensGraph graph) {
        super(id, label, graph);
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (this.removed) return VertexProperty.empty();

        if (this.properties != null && this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        final Object idValue = optionalId.isPresent() ?
                graph.vertexPropertyIdManager.convert(optionalId.get()) :
                graph.vertexPropertyIdManager.getNextId(graph);

        final VertexProperty<V> vertexProperty = new AgensVertexProperty<V>(idValue, this, key, value);

        if (null == this.properties) this.properties = new HashMap<>();
        final List<VertexProperty> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(vertexProperty);
        this.properties.put(key, list);
        AgensHelper.autoUpdateIndex(this, key, value, null);
        ElementHelper.attachProperties(vertexProperty, keyValues);
        return vertexProperty;
    }

    @Override
    public Set<String> keys() {
        if (null == this.properties) return Collections.emptySet();
        return AgensHelper.inComputerMode((AgensGraph) graph()) ?
                Vertex.super.keys() :
                this.properties.keySet();
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id);
        return AgensHelper.addEdge(this.graph, this, (AgensVertex) vertex, label, keyValues);
    }

    @Override
    public void remove() {
        final List<Edge> edges = new ArrayList<>();
        this.edges(Direction.BOTH).forEachRemaining(edges::add);
        edges.stream().filter(edge -> !((AgensEdge) edge).removed).forEach(Edge::remove);
        this.properties = null;
        AgensHelper.removeElementIndex(this);
        this.graph.vertices.remove(this.id);
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final Iterator<Edge> edgeIterator = (Iterator) AgensHelper.getEdges(this, direction, edgeLabels);
        return edgeIterator;
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return AgensHelper.inComputerMode(this.graph) ?
                direction.equals(Direction.BOTH) ?
                        IteratorUtils.concat(
                                IteratorUtils.map(this.edges(Direction.OUT, edgeLabels), Edge::inVertex),
                                IteratorUtils.map(this.edges(Direction.IN, edgeLabels), Edge::outVertex)) :
                        IteratorUtils.map(this.edges(direction, edgeLabels), edge -> edge.vertices(direction.opposite()).next()) :
                (Iterator) AgensHelper.getVertices(this, direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        if (this.removed) return Collections.emptyIterator();

        if (null == this.properties) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            final List<VertexProperty> properties = this.properties.getOrDefault(propertyKeys[0], Collections.emptyList());
            if (properties.size() == 1) {
                return IteratorUtils.of(properties.get(0));
            } else if (properties.isEmpty()) {
                return Collections.emptyIterator();
            } else {
                return (Iterator) new ArrayList<>(properties).iterator();
            }
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList()).iterator();
    }

    ////////////////////////////////

    @Override
    public ElasticVertexWrapper getBaseVertex() {
        return (ElasticVertexWrapper) this.baseElement;
    }
}