package net.bitnine.agenspop.graph.structure.trait;

import com.google.common.collect.Iterables;
import net.bitnine.agenspop.elastic.ElasticGraphAPI;
import net.bitnine.agenspop.elastic.document.ElasticEdgeDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import net.bitnine.agenspop.graph.process.traversal.LabelP;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensHelper;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.graph.structure.AgensVertexProperty;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class SimpleAgensTrait implements AgensTrait {

    private static final SimpleAgensTrait INSTANCE = new SimpleAgensTrait();
    public static SimpleAgensTrait instance() {
        return INSTANCE;
    }

    private SimpleAgensTrait() { }

    private final static Predicate TRUE_PREDICATE = x -> true;

    @Override
    public Predicate<ElasticVertex> getVertexPredicate() {
        return TRUE_PREDICATE;
    }
    @Override
    public Predicate<ElasticEdge> getEdgePredicate() {
        return TRUE_PREDICATE;
    }

    @Override
    public void removeVertex(final AgensVertex vertex) {
        ElasticGraphAPI api = ((AgensGraph)vertex.graph()).getBaseGraph();
        try {
            final ElasticVertex node = vertex.getBaseVertex();

            // @Todo node.relationships(Direction.BOTH)
            final ArrayList<ElasticEdge> relationships = new ArrayList<>();
            Iterables.addAll(relationships, api.edgesBySid(node.getEid()));
            Iterables.addAll(relationships, api.edgesByTid(node.getEid()));
            for (final ElasticEdge relationship : relationships) {
                api.deleteE( relationship );
            }
            api.deleteV( node );

        } catch (final IllegalStateException ignored) {
            // this one happens if the vertex is still chilling in the tx
        } catch (final RuntimeException ex) {
            if (!AgensHelper.isNotFound(ex)) throw ex;
            // this one happens if the vertex is committed
        }
    }

    @Override
    public <V> VertexProperty<V> getVertexProperty(final AgensVertex vertex, final String key) {
        return vertex.getBaseVertex().hasProperty(key) ? new AgensVertexProperty<>(vertex, key, (V) vertex.getBaseVertex().getProperty(key)) : VertexProperty.<V>empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> getVertexProperties(final AgensVertex vertex, final String... keys) {
        return (Iterator) IteratorUtils.stream(vertex.getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, keys))
                .map(key -> new AgensVertexProperty<>(vertex, key, (V) vertex.getBaseVertex().getProperty(key))).iterator();
    }

    @Override
    public <V> VertexProperty<V> setVertexProperty(final AgensVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        try {
            vertex.getBaseVertex().setProperty(key, value);
            return new AgensVertexProperty<>(vertex, key, value);
        } catch (final IllegalArgumentException iae) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, iae);
        }
    }

    @Override
    public VertexProperty.Cardinality getCardinality(final String key) {
        return VertexProperty.Cardinality.single;
    }

    @Override
    public boolean supportsMultiProperties() {
        return false;
    }

    @Override
    public boolean supportsMetaProperties() {
        return false;
    }

    @Override
    public void removeVertexProperty(final AgensVertexProperty vertexProperty) {
        final ElasticVertex node = ((AgensVertex) vertexProperty.element()).getBaseVertex();
        if (node.hasProperty(vertexProperty.key()))
            node.removeProperty(vertexProperty.key());
    }

    @Override
    public <V> Property<V> setProperty(final AgensVertexProperty vertexProperty, final String key, final V value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Property<V> getProperty(final AgensVertexProperty vertexProperty, final String key) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> getProperties(final AgensVertexProperty vertexProperty, final String... keys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Iterator<Vertex> lookupVertices(final AgensGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        // ids are present, filter on them first
        if (ids.length > 0)
            return IteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));
        ////// do index lookups //////
        graph.tx().readWrite();
        // get a label being search on
        Optional<String> label = hasContainers.stream()
                .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
                .map(hasContainer -> (String) hasContainer.getValue())
                .findAny();
        if (!label.isPresent())
            label = hasContainers.stream()
                    .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
                    .filter(hasContainer -> hasContainer.getPredicate() instanceof LabelP)
                    .map(hasContainer -> (String) hasContainer.getValue())
                    .findAny();

        if (label.isPresent()) {
            //
            // @Todo hasSchemaIndex(), verticesByLabelAndPropsKeyAndValue()

            // find a vertex by label and key/value
//            for (final HasContainer hasContainer : hasContainers) {
//                if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey().equals(T.label.getAccessor())) {
//                    if (graph.getBaseGraph().hasSchemaIndex(label.get(), hasContainer.getKey())) {
//                        return IteratorUtils.stream(graph.getBaseGraph().verticesByLabelAndPropsKeyAndValue(label.get(), hasContainer.getKey(), hasContainer.getValue()))
//                                .map(node -> (Vertex) new AgensVertex(node, graph))
//                                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
//                    }
//                }
//            }
            // find a vertex by label
            return IteratorUtils.stream(graph.getBaseGraph().verticesByLabel(label.get()))
                    .map(node -> (Vertex) new AgensVertex(node, graph))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan
            return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
        }
    }    
}
