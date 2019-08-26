package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedProperty;

import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensProperty<V> implements Property<V>, WrappedProperty<BaseProperty> {

    protected final BaseProperty propertyBase;
    protected final Element element;
    protected boolean removed = false;

    // 외부 생성
    public AgensProperty(final Element element, final String key, final V value) {
        Objects.requireNonNull(value, "AgensProperty.value might be null");
        propertyBase = ((AgensGraph)element.graph()).api.createProperty(key, value);
        this.element = element;
        ((AgensElement)this.element).getBaseElement().setProperty(this.propertyBase);
    }
    // 내부 생성
    public AgensProperty(final Element element, final BaseProperty propertyBase) {
        this.propertyBase = propertyBase;
        this.element = element;
        ((AgensElement)this.element).getBaseElement().setProperty(this.propertyBase);
    }

    @Override
    public BaseProperty getBaseProperty() { return this.propertyBase; }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public String key() {
        return this.propertyBase.key();
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
        return "p["+key()+":"+this.propertyBase.value()+"]";
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
        final BaseElement entity = ((AgensElement) this.element).getBaseElement();
        if (entity.hasProperty(this.key())) {
            entity.removeProperty(this.key());
        }
    }
}
