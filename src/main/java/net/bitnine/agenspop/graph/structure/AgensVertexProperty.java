package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.basegraph.model.BaseProperty;
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
public class AgensVertexProperty<V> implements VertexProperty<V>, WrappedVertexProperty<BaseProperty> {

    protected final BaseProperty propertyBase;
    protected final AgensVertex vertex;

    // case1 : AgensGraph 외부인 사용자단으로부터 생성되는 경우
    public AgensVertexProperty(final AgensVertex vertex, final String key, final V value) {
        Objects.requireNonNull(value, "AgensVertexProperty.value might be null");
        this.propertyBase = ((AgensGraph)vertex.graph()).api.createProperty(key, value);
        this.vertex = vertex;
        this.vertex.baseElement.setProperty(this.propertyBase);
    }

    // case2 : baseGraphAPI 로부터 생성되는 경우
    public AgensVertexProperty(final AgensVertex vertex, final BaseProperty propertyBase) {
        this.propertyBase = propertyBase;
        this.vertex = vertex;
        this.vertex.baseElement.setProperty(this.propertyBase);
    }

    @Override
    public BaseProperty getBaseVertexProperty(){
        return this.propertyBase;
    }

    ////////////////////////////////////

    @Override
    public Vertex element() { return this.vertex; }

    @Override
    public Object id() {
        // TODO: ElasticVertex needs a better ID system for VertexProperties
        return (String) "vp_"+this.vertex.id().hashCode()+"_"+this.key().hashCode();
    }

    @Override
    public String key() { return this.propertyBase.key(); }

    // **NOTE: Cardinality.single 때문에 사용되어서는 안됨
    @Override
    public Set<String> keys() {
        if(null == this.propertyBase) return Collections.emptySet();
        final Set<String> keys = new HashSet<>();
        keys.add(this.propertyBase.key());
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
        this.vertex.properties.remove(propertyBase.key());
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
        return "p["+key()+":"+this.propertyBase.value()+"]";
    }
}
