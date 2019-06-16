package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.elastic.model.ElasticElement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AgensElement implements Element, WrappedElement<ElasticElement> {

    protected boolean removed = false;

    protected final AgensGraph graph;
    protected final ElasticElement baseElement;

    protected AgensElement(final ElasticElement baseElement, final AgensGraph graph) {
        this.baseElement = baseElement;
        this.graph = graph;
    }

    @Override
    public ElasticElement getBaseElement() {
        return this.baseElement;
    }

    ///////////////////////////////////

    @Override
    public Object id() {
        this.graph.tx().readWrite();
        return this.baseElement.getEid();
    }

    @Override
    public String label() {
        this.graph.tx().readWrite();
        return this.baseElement.getLabel();
    }

    @Override
    public Set<String> keys() {
        this.graph.tx().readWrite();
        final Set<String> keys = new HashSet<>();
        for (final String key : this.baseElement.getKeys()) {
            if (!Graph.Hidden.isHidden(key))
                keys.add(key);
        }
        return Collections.unmodifiableSet(keys);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }

}