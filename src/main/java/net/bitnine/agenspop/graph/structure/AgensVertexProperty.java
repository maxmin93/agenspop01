package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AgensVertexProperty<V> implements VertexProperty<V> {

    protected ElasticVertex vertexPropertyBase;
    private final AgensVertex vertex;
    private final String key;
    private final V value;

    public AgensVertexProperty(final AgensVertex vertex, final String key, final V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        this.vertexPropertyBase = null;
    }

    public AgensVertexProperty(final AgensVertex vertex, final String key, final V value, final ElasticVertex vertexPropertyBase) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        this.vertexPropertyBase = vertexPropertyBase;
    }

    public AgensVertexProperty(final AgensVertex vertex, final ElasticVertex vertexPropertyBase) {
        this.vertex = vertex;
        this.key = (String) vertexPropertyBase.getProperty(T.key.getAccessor());
        this.value = (V) vertexPropertyBase.getProperty(T.value.getAccessor());
        this.vertexPropertyBase = vertexPropertyBase;
    }

    ////////////////////////////////////

    @Override
    public Vertex element() { return this.vertex; }

    @Override
    public Object id() {
        // TODO: ElasticVertex needs a better ID system for VertexProperties
        return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public String key() { return this.key; }
    @Override
    public Set<String> keys() {
        if(null == this.vertexPropertyBase) return Collections.emptySet();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.vertexPropertyBase.getKeys()) {
            if (!Graph.Hidden.isHidden(key) && !key.equals(this.key))
                keys.add(key);
        }
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public V value() throws NoSuchElementException { return this.value; }

    @Override
    public boolean isPresent() { return null != this.value; }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        this.vertex.graph.tx().readWrite();
        return this.vertex.graph.trait.getProperties(this, propertyKeys);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        this.vertex.graph.tx().readWrite();
        ElementHelper.validateProperty(key, value);
        return this.vertex.graph.trait.setProperty(this, key, value);
    }

    @Override
    public void remove() {
        this.vertex.graph.tx().readWrite();
        this.vertex.graph.trait.removeVertexProperty(this);
        this.vertexPropertyBase= null;
    }


    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }
}
