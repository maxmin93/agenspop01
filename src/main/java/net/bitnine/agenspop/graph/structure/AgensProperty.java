package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.elastic.document.ElasticPropertyDocument;
import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensProperty<V> implements Property<V>, WrappedProperty<ElasticProperty> {

    protected final ElasticProperty propertyBase;
    protected final Element element;
    protected boolean removed = false;

    public AgensProperty(final Element element, final String key, final V value) {
        Objects.requireNonNull(value, "AgensProperty.value might be null");
        this.element = element;
        this.propertyBase = new ElasticPropertyDocument(key, value.getClass().getName(), value );
        // add property to ElasticElement
        ((AgensElement)this.element).getBaseElement().setProperty(this.propertyBase);
    }

    @Override
    public ElasticProperty getBaseProperty() { return this.propertyBase; }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public String key() {
        return this.propertyBase.getKey();
    }

    @Override
    public V value() {
        return (V)this.propertyBase.value();
    }

    @Override
    public boolean isPresent() {
        return this.propertyBase.isPresent();
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
        this.element.graph().tx().readWrite();
        final ElasticElement entity = ((AgensElement) this.element).getBaseElement();
        if (entity.hasProperty(this.key())) {
            entity.removeProperty(this.key());
        }
    }
}
