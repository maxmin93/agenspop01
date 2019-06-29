package net.bitnine.agenspop.graph.structure.trait;

import net.bitnine.agenspop.elastic.ElasticGraphAPI;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
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
import java.util.stream.Collectors;

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
        ElasticVertex baseVertex = vertex.getBaseVertex();
        try {
            baseVertex.delete();            // marking deleted
            api.deleteVertex( baseVertex ); // remove ElasticVertex
        } catch (final RuntimeException ex) {
            if (!AgensHelper.isNotFound(ex)) throw ex;
        }
    }

    @Override
    public <V> VertexProperty<V> getVertexProperty(final AgensVertex vertex, final String key) {
        if( vertex.getBaseVertex().hasProperty(key) ){
            Optional<ElasticProperty> pBase = vertex.getBaseVertex().getProperty(key);
            if( !pBase.isPresent() ) VertexProperty.<V>empty();
            VertexProperty<V> p = new AgensVertexProperty(vertex, pBase.get());
            return p;
        }
        return VertexProperty.<V>empty();
    }

    @Override
    public <V> Iterator<VertexProperty<V>> getVertexProperties(final AgensVertex vertex, final String... keys) {
        List<String> validKeys = IteratorUtils.stream(vertex.getBaseVertex().getKeys())
                .filter(key -> ElementHelper.keyExists(key, keys)).collect(Collectors.toList());

        List<VertexProperty<V>> properties = new ArrayList<>();
        for( String key : validKeys ) {
            VertexProperty<V> p = vertex.property(key);
            properties.add(p);
        }
        return properties.iterator();
    }

    @Override
    public <V> VertexProperty<V> setVertexProperty(final AgensVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        // 하나의 property 는 하나의 객체만 담는다 (collection 아님) ==> single
        if (cardinality != VertexProperty.Cardinality.single)
            throw VertexProperty.Exceptions.multiPropertiesNotSupported();
        if (keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        // ElasticElement 를 통해 ElasticProperty 생성, 추가하고 AgensVertexProperty 를 반환
        try {
            // 지원하지 않는 value.getClass() 에 대해 NoSuchElementException 발생 가능
            // add property to ElasticVertex by setProperty(key, value)
            return new AgensVertexProperty<>(vertex, key, value);
        } catch (final Exception ex) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value, ex);
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
        final ElasticVertex base = ((AgensVertex) vertexProperty.element()).getBaseVertex();
        if (base.hasProperty(vertexProperty.key()))
            base.removeProperty(vertexProperty.key());
    }

    //////////////////////////////////////////////////

    @Override
    public <V> Property<V> setProperty(final AgensVertexProperty vertexProperty, final String key, final V value) {
        System.out.println("  !! setProperty() in "+this.getClass().getName());
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Property<V> getProperty(final AgensVertexProperty vertexProperty, final String key) {
        System.out.println("  !! getProperty() in "+this.getClass().getName());
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <V> Iterator<Property<V>> getProperties(final AgensVertexProperty vertexProperty, final String... keys) {
        System.out.println("  - getProperties() in "+this.getClass().getName());
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Iterator<Vertex> lookupVertices(final AgensGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
        System.out.println("  - lookupVertices() in "+this.getClass().getName());
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
//                    .filter(hasContainer -> hasContainer.getPredicate() instanceof LabelP)
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
            return IteratorUtils.stream(graph.getBaseGraph().findVertices(graph.name(), label.get()))
                    .map(node -> (Vertex) new AgensVertex(node, graph))
                    .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
        } else {
            // linear scan
            return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
        }
    }    
}
