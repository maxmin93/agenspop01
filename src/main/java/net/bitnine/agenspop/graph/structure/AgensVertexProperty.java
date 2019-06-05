package net.bitnine.agenspop.graph.structure;


import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AgensVertexProperty<V> extends AgensElement implements VertexProperty<V> {

    protected Map<String, Property> properties;
    private final AgensVertex vertex;
    private final String key;
    private final V value;

    /**
     * This constructor will not validate the ID type against the {@link Graph}.  It will always just use a
     * {@code Long} for its identifier.
     */
    public AgensVertexProperty(final AgensVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(((AgensGraph) vertex.graph()).vertexPropertyIdManager.getNextId((AgensGraph) vertex.graph()), key, vertex.graph);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    /**
     * Use this constructor to construct {@link VertexProperty} instances for {@link AgensGraph} where the {@code id}
     * can be explicitly set and validated against the expected data type.
     */
    public AgensVertexProperty(final Object id, final AgensVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key, vertex.graph);
        this.vertex = vertex;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public Set<String> keys() {
        return null == this.properties ? Collections.emptySet() : this.properties.keySet();
    }

    @Override
    public <U> Property<U> property(final String key) {
        return null == this.properties ? Property.<U>empty() : this.properties.getOrDefault(key, Property.<U>empty());
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        if (this.removed) throw elementAlreadyRemoved(VertexProperty.class, id);
        final Property<U> property = new AgensProperty<>(this, key, value);
        if (this.properties == null) this.properties = new HashMap<>();
        this.properties.put(key, property);
        return property;
    }

    @Override
    public Vertex element() {
        return this.vertex;
    }

    @Override
    public void remove() {
        if (null != this.vertex.properties && this.vertex.properties.containsKey(this.key)) {
            this.vertex.properties.get(this.key).remove(this);
            if (this.vertex.properties.get(this.key).size() == 0) {
                this.vertex.properties.remove(this.key);
                AgensHelper.removeIndex(this.vertex, this.key, this.value);
            }
            final AtomicBoolean delete = new AtomicBoolean(true);
            this.vertex.properties(this.key).forEachRemaining(property -> {
                if (property.value().equals(this.value))
                    delete.set(false);
            });
            if (delete.get()) AgensHelper.removeIndex(this.vertex, this.key, this.value);
            this.properties = null;
            this.removed = true;
        }
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        if (null == this.properties) return Collections.emptyIterator();
        if (propertyKeys.length == 1) {
            final Property<U> property = this.properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) this.properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();
    }
}