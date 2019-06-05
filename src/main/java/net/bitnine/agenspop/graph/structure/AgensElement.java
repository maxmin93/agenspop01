package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.graph.structure.es.ElasticElementWrapper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;

public abstract class AgensElement implements Element, WrappedElement<ElasticElementWrapper> {

    protected final Object id;
    protected final String label;
    protected boolean removed = false;

    protected final AgensGraph graph;
    protected final ElasticElementWrapper baseElement = null;

    protected AgensElement(final Object id, final String label, AgensGraph graph) {
        this.id = id;
        this.label = label;
        this.graph = graph;
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }

    ///////////////////////////////////

    @Override
    public ElasticElementWrapper getBaseElement() {
        return this.baseElement;
    }
}