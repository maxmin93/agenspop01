package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.elastic.document.ElasticPropertyDocument;
import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AgensVertexProperty<V> implements VertexProperty<V>, WrappedVertexProperty<ElasticProperty> {

    protected final ElasticProperty propertyBase;
    protected final AgensVertex vertex;

    public AgensVertexProperty(final AgensVertex vertex, final String key, final V value) {
        Objects.requireNonNull(value, "AgensVertexProperty.value might be null");
        this.vertex = vertex;
        this.propertyBase = new ElasticPropertyDocument(key, value.getClass().getName(), (Object)value );
        // add property to ElasticVertex
        this.vertex.baseElement.setProperty(this.propertyBase);
    }

    public AgensVertexProperty(final AgensVertex vertex, final ElasticProperty propertyBase) {
        this.vertex = vertex;
        this.propertyBase = propertyBase;
        this.vertex.baseElement.setProperty(this.propertyBase);
    }

    @Override
    public ElasticProperty getBaseVertexProperty(){
        return this.propertyBase;
    }

    ////////////////////////////////////

    @Override
    public Vertex element() { return this.vertex; }

    @Override
    public Object id() {
        // TODO: ElasticVertex needs a better ID system for VertexProperties
        return (long) (this.key().hashCode() + this.vertex.id().hashCode());
    }

    @Override
    public String key() { return this.propertyBase.getKey(); }

    @Override
    public Set<String> keys() {
        if(null == this.propertyBase) return Collections.emptySet();
        final Set<String> keys = new HashSet<>();
        keys.add(this.propertyBase.getKey());
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public V value() throws NoSuchElementException { return (V)this.propertyBase.value(); }

    @Override
    public boolean isPresent() { return this.propertyBase.isPresent(); }

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
