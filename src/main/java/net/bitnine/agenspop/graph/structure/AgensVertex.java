package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.elastic.document.ElasticPropertyDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensVertex extends AgensElement implements Vertex, WrappedVertex<ElasticVertex> {

    protected Map<String, VertexProperty> properties;
//    protected Map<String, Set<Edge>> outEdges;
//    protected Map<String, Set<Edge>> inEdges;

    public AgensVertex(final ElasticVertex vertex, final AgensGraph graph) {
        super(vertex, graph);
    }

    public AgensVertex(final Object id, final String label, final AgensGraph graph) {
        super(graph.baseGraph.createVertex(id.toString(), label), graph);
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
        if (null == inVertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id());
        return AgensHelper.addEdge(this.graph, this, (AgensVertex) inVertex, label, keyValues);
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        // remove connected AgensEdges and AgensVertex
        final Iterable<ElasticEdge> edges = graph.baseGraph.findEdgesOfVertexWithDirection(id().toString(),Direction.BOTH);
        for( ElasticEdge edge : edges ) {
            edge.delete();
            graph.baseGraph.deleteEdge(edge);
        }
        // post processes of remove vertex : properties, graph, marking
        this.properties = null;
        this.removed = true;
        // remove connected ElasticEdges and ElasticVertex
        this.graph.trait.removeVertex(this);
    }

    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality
            , final String key, final V value, final Object... keyValues) {

        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id());
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        if (cardinality.equals(VertexProperty.Cardinality.single) && properties != null )
            properties.remove(key);

        this.graph.tx().readWrite();
        final VertexProperty<V> vertexProperty = this.graph.trait.setVertexProperty(this, cardinality, key, value, keyValues);
        if (null == this.properties) this.properties = new ConcurrentHashMap<>();
        this.properties.put(key, vertexProperty);
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(VertexProperty.Cardinality.single, key, value);
    }

    // @Todo remove trait codes about VertexProperty
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

    // 정점의 이웃 정점들 (방향성, 연결간선의 라벨셋)
    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
//        return (Iterator) AgensHelper.getVertices(this, direction, edgeLabels);

        this.graph.tx().readWrite();
        return new Iterator<Vertex>() {
            final Iterator<ElasticVertex> neighborIterator = IteratorUtils.filter(
                    graph.baseGraph.findNeighborVerticesWithDirectionAndLabels(id().toString(), direction, edgeLabels).iterator()
                    , graph.trait.getVertexPredicate());

            @Override
            public boolean hasNext() {
                return this.neighborIterator.hasNext();
            }
            @Override
            public AgensVertex next() {
                return new AgensVertex(this.neighborIterator.next(), graph);
            }
        };
    }

    // 정점의 연결간선들 (방향, 연결간선의 라벨셋)
    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
//        final Iterator<Edge> edgeIterator = (Iterator) AgensHelper.getEdges(this, direction, edgeLabels);
//        return edgeIterator;

        this.graph.tx().readWrite();
        return new Iterator<Edge>() {
            final Iterator<ElasticEdge> relationshipIterator = IteratorUtils.filter(
                graph.baseGraph.findEdgesOfVertexWithDirectionAndLabels(id().toString(), direction, edgeLabels).iterator()
                , graph.trait.getEdgePredicate());

            @Override
            public boolean hasNext() {
                return this.relationshipIterator.hasNext();
            }
            @Override
            public AgensEdge next() {
                return new AgensEdge(this.relationshipIterator.next(), graph);
            }
        };
    }

    //////////////////////////////////////////////////////////////////////////////////////

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}