package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.elastic.document.ElasticVertexDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensEdge extends AgensElement implements Edge, WrappedEdge<ElasticEdge> {

    protected Map<String, Property> properties;
//    protected final Vertex outVertex;       // sourceV
//    protected final Vertex inVertex;        // targetV

    public AgensEdge(final ElasticEdge edge, final AgensGraph graph) {
        super(edge, graph);
        this.properties = edge.getProperties().stream().collect(
                Collectors.toMap(r->((ElasticProperty)r).getKey(), r->(Property)r));
    }

    public AgensEdge(final Object id, final AgensVertex outVertex, final String label, final AgensVertex inVertex) {
        super(
            outVertex.graph.baseGraph.createEdge(id.toString(), label
                , outVertex.baseElement.getId(), inVertex.baseElement.getId()
            )                           // elasticedge
            , outVertex.graph           // graph
        );
    }

    @Override
    public Vertex outVertex() {     // source v of edge
        Optional<? extends ElasticVertex> v = this.graph.baseGraph.getVertexById(getBaseEdge().getSid());
        return (Vertex) v.orElse(null);
    }

    @Override
    public Vertex inVertex() {      // target v of edge
        Optional<? extends ElasticVertex> v = this.graph.baseGraph.getVertexById(getBaseEdge().getTid());
        return (Vertex) v.orElse(null);
    }

    @Override
    public ElasticEdge getBaseEdge() {
        return (ElasticEdge) this.baseElement;
    }

    ////////////////////////////////

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        Iterable<String> keys = this.baseElement.getKeys();
        Iterator<String> filter = IteratorUtils.filter(keys.iterator(),
                key -> ElementHelper.keyExists(key, propertyKeys));
        return IteratorUtils.map(filter,
                key -> new AgensProperty<>(this, key, (V) this.baseElement.getProperty(key)));
    }

    @Override
    public <V> Property<V> property(final String key) {
        this.graph.tx().readWrite();
        // properties 에 없으면 가져오기
        if (this.baseElement.hasProperty(key))
            return new AgensProperty<>(this, key, (V) this.baseElement.getProperty(key));
        else
            return Property.empty();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);

        this.graph.tx().readWrite();
        final AgensProperty<V> property = new AgensProperty<V>(this, key, value);
        if (null == this.properties) this.properties = new ConcurrentHashMap<>();
        this.properties.put(key, property);
        return property;
    }

    ////////////////////////////////

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        // post processes of remove vertex : properties, graph, marking
        this.properties = null;
        this.removed = true;

        ElasticEdge baseEdge = this.getBaseEdge();
        try {
            baseEdge.delete();                          // marking deleted
            this.graph.baseGraph.deleteEdge(baseEdge);  // delete ElasticEdgeDocument
        }
        catch (RuntimeException e) {
            if (!AgensHelper.isNotFound(e)) throw e;
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }


    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex());
            case IN:
                return IteratorUtils.of(this.inVertex());
            default:
                return IteratorUtils.of(this.outVertex(), this.inVertex());
        }
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.getBaseEdge().getLabel();
    }
}