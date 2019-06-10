package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.elastic.model.ElasticEdge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedEdge;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensEdge extends AgensElement implements Edge, WrappedEdge<ElasticEdge> {

//    protected Map<String, Property> properties;
    protected final Vertex inVertex;
    protected final Vertex outVertex;

    public AgensEdge(final ElasticEdge edge, final AgensGraph graph) {
        super(edge, graph);
        this.inVertex = null;       // 나중에 : vertexById 로 찾아 넣기
        this.outVertex = null;      // 나중에 : vertexById 로 찾아 넣기
    }

//    protected AgensEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
//        super(id, label, (AgensGraph)inVertex.graph());
//        this.outVertex = outVertex;
//        this.inVertex = inVertex;
//        AgensHelper.autoUpdateIndex(this, T.label.getAccessor(), this.label, null);
//    }

    @Override
    public Vertex outVertex() {     // source v of edge
//        return new AgensVertex(this.getBaseEdge().getSid(), this.graph);
        return (Vertex)this.outVertex;
    }

    @Override
    public Vertex inVertex() {      // target v of edge
//        return new AgensVertex(this.getBaseEdge().getTid(), this.graph);
        return (Vertex)this.inVertex;
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
        if (this.baseElement.hasProperty(key))
            return new AgensProperty<>(this, key, (V) this.baseElement.getProperty(key));
        else
            return Property.empty();
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        try {
            this.baseElement.setProperty(key, value);
            return new AgensProperty<>(this, key, value);
        } catch (final IllegalArgumentException e) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, e);
        }
    }

    ////////////////////////////////

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        try {
            this.baseElement.delete();
        }
        catch (IllegalStateException ignored) {
            // NotFoundException happens if the edge is committed
            // IllegalStateException happens if the edge is still chilling in the tx
        }
//        catch (RuntimeException e) {
//            if (!AgensHelper.isNotFound(e)) throw e;
//        }

        final AgensVertex outVertex = (AgensVertex) this.outVertex;
        final AgensVertex inVertex = (AgensVertex) this.inVertex;

        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }

        AgensHelper.removeElementIndex(this);
        ((AgensGraph) this.graph()).edges.remove(this.id());
        this.removed = true;
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
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public Graph graph() {
        return this.inVertex.graph();
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.getBaseEdge().getLabel();
    }
}