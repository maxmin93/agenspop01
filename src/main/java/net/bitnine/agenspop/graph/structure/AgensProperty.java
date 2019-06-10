package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensProperty<V> implements Property<V> {

    protected final Element element;
    protected final String key;
    protected final AgensGraph graph;
    protected V value;
    protected boolean removed = false;

    public AgensProperty(final Element element, final String key, final V value) {
        this.element = element;
        this.graph = (AgensGraph)element.graph();
        this.key = key;
        this.value = value;
    }

    @Override
    public Element element() {
        return this.element;
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
        return null != this.value;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        if (this.removed) return;

        this.removed = true;
        this.graph.tx().readWrite();
        final ElasticElement entity = ((AgensElement) this.element).getBaseElement();
        if (entity.hasProperty(this.key)) {
            entity.removeProperty(this.key);
        }
    }
}
