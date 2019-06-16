package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensVertex extends AgensElement implements Vertex, WrappedVertex<ElasticVertex> {

    public static final String LABEL_DELIMINATOR = "::";

    protected Map<String, List<VertexProperty>> properties;
    protected Map<String, Set<Edge>> outEdges;
    protected Map<String, Set<Edge>> inEdges;

    public AgensVertex(final ElasticVertex vertex, final AgensGraph graph) {
        super(vertex, graph);
    }

    public AgensVertex(final Object id, final String label, final AgensGraph graph) {
        super(
            graph.baseGraph.createVertex((Integer)id, graph.graphName, label)
            , graph
        );
    }

    @Override
    public Graph graph(){ return this.graph; }

    @Override
    public ElasticVertex getBaseVertex() {
        return (ElasticVertex) this.baseElement;
    }

    ////////////////////////////////////

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final Edge edge;

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }
        
        this.graph.tx().readWrite();
        final ElasticEdge elasticEdge = graph.baseGraph.createEdge(
                (Integer)idValue, graph.graphName, label
                , (Integer)this.id(), (Integer)inVertex.id()
        );
        edge = new AgensEdge(elasticEdge, this.graph);              
        ElementHelper.attachProperties(edge, keyValues);
        
        return edge;
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        this.graph.trait.removeVertex(this);
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (keyValues != null && keyValues.length > 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        if (cardinality.equals(VertexProperty.Cardinality.single))
            properties.remove(key);
        ElementHelper.validateProperty(key, value);
        this.graph.tx().readWrite();
        return this.property(key, value);

//        if (ElementHelper.getIdValue(keyValues).isPresent())
//            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
//        return this.graph.trait.setVertexProperty(this, cardinality, key, value, keyValues);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(VertexProperty.Cardinality.single, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        this.graph.tx().readWrite();
        return this.graph.trait.getVertexProperty(this, key);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        this.graph.tx().readWrite();
        return this.graph.trait.getVertexProperties(this, propertyKeys);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return (Iterator) AgensHelper.getVertices(this, direction, edgeLabels);
        /*
        this.graph.tx().readWrite();
        return new Iterator<Vertex>() {
            final Iterator<ElasticEdge> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                Direction.BOTH == direction ?
                        IteratorUtils.concat(getBaseVertex().relationships(AgensHelper.mapDirection(OUT)).iterator(),
                                getBaseVertex().relationships(AgensHelper.mapDirection(IN)).iterator()) :
                        getBaseVertex().relationships(AgensHelper.mapDirection(direction)).iterator() :
                Direction.BOTH == direction ?
                        IteratorUtils.concat(getBaseVertex().relationships(AgensHelper.mapDirection(OUT), (edgeLabels)).iterator(),
                                getBaseVertex().relationships(AgensHelper.mapDirection(IN), (edgeLabels)).iterator()) :
                        getBaseVertex().relationships(AgensHelper.mapDirection(direction), (edgeLabels)).iterator(), graph.trait.getRelationshipPredicate());

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public AgensVertex next() {
                return new AgensVertex(this.relationshipIterator.next().other(getBaseVertex()), graph);
            }
        };
        */
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        final Iterator<Edge> edgeIterator = (Iterator) AgensHelper.getEdges(this, direction, edgeLabels);
        return edgeIterator;
        /*
        this.graph.tx().readWrite();
        return new Iterator<Edge>() {
            final Iterator<ElasticEdge> relationshipIterator = IteratorUtils.filter(0 == edgeLabels.length ?
                    Direction.BOTH == direction ?
                            IteratorUtils.concat(getBaseVertex().relationships(AgensHelper.mapDirection(OUT)).iterator(),
                                    getBaseVertex().relationships(AgensHelper.mapDirection(IN)).iterator()) :
                            getBaseVertex().relationships(AgensHelper.mapDirection(direction)).iterator() :
                    Direction.BOTH == direction ?
                            IteratorUtils.concat(getBaseVertex().relationships(AgensHelper.mapDirection(OUT), (edgeLabels)).iterator(),
                                    getBaseVertex().relationships(AgensHelper.mapDirection(IN), (edgeLabels)).iterator()) :
                            getBaseVertex().relationships(AgensHelper.mapDirection(direction), (edgeLabels)).iterator(), graph.trait.getRelationshipPredicate());

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }

            @Override
            public AgensEdge next() {
                return new AgensEdge(this.relationshipIterator.next(), graph);
            }
        };
        */
    }

    //////////////////////////////////////////////////////////////////////////////////////

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}