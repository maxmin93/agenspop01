package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AgensVertex extends AgensElement implements Vertex, WrappedVertex<BaseVertex> {

    public AgensVertex(final AgensGraph graph, final BaseVertex vertex) {
        super(graph, vertex);
    }

    public AgensVertex(final AgensGraph graph, final Object id, final String label) {
        super(graph, graph.api.createVertex(graph.name(), id.toString(), label));
    }

    @Override
    public Graph graph(){ return this.graph; }

    @Override
    public BaseVertex getBaseVertex() {
        return (BaseVertex) this.baseElement;
    }

    ////////////////////////////////////

    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        if (inVertex == null) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id());
        return AgensHelper.addEdge(this.graph, this, (AgensVertex) inVertex, label, keyValues);
    }

    @Override
    public void remove() {
        this.graph.tx().readWrite();
        // 1) remove connected AgensEdges and 2) remove AgensVertex
        final Iterable<BaseEdge> edges = graph.api.findEdgesOfVertex(graph.name(), id().toString(), Direction.BOTH);
        for( BaseEdge edge : edges ) {
            graph.api.dropEdge(edge.getId());
        }
        // post processes of remove vertex : properties, graph, marking
        this.removed = true;
        // remove connected ElasticEdges and ElasticVertex
        this.graph.trait.removeVertex(this);
    }

    // **NOTE: Cardinality.single 만 다룬다 ==> multi(set or list) 인 경우 Exception 처리
    //
    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality
            , final String key, final V value, final Object... keyValues) {

        if (this.removed) throw elementAlreadyRemoved(Vertex.class, this.id());
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);

        this.graph.tx().readWrite();
        final VertexProperty<V> vertexProperty = new AgensVertexProperty<>(this, key, value);
                // this.graph.trait.setVertexProperty(this, cardinality, key, value, keyValues);
        // rest keyValues
        ElementHelper.attachProperties(vertexProperty, keyValues);
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
        Iterable<BaseVertex> bases = graph.api.findNeighborVertices(id().toString(), direction, edgeLabels);
        final List<Vertex> vertices = new ArrayList<>();
        for( BaseVertex base : bases ){
            vertices.add( new AgensVertex(base, graph));
        }

        return vertices.iterator();
    }

    // 정점의 연결간선들 (방향, 연결간선의 라벨셋)
    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
//        final Iterator<Edge> edgeIterator = (Iterator) AgensHelper.getEdges(this, direction, edgeLabels);
//        return edgeIterator;

        this.graph.tx().readWrite();
        Iterable<BaseEdge> bases = graph.api.findEdgesOfVertex(id().toString(), direction, edgeLabels);
        final List<Edge> edges = new ArrayList<>();
        for( BaseEdge base : bases ) edges.add( new AgensEdge(base, graph));
        return edges.iterator();
    }

    //////////////////////////////////////////////////////////////////////////////////////
    // **참고 https://github.com/rayokota/hgraphdb

    public Iterator<Edge> edges(final Direction direction, final String label, final String key, final Object value) {
        this.graph.tx().readWrite();
        Iterable<BaseEdge> bases = graph.api
                .findEdgesOfVertex(id().toString(), direction, label, key, value);
        final List<Edge> edges = new ArrayList<>();
        for( BaseEdge base : bases ) edges.add( new AgensEdge(base, graph));
        return edges.iterator();
    }

/*
    public Iterator<Edge> edgesInRange(final Direction direction, final String label, final String key,
                                       final Object inclusiveFromValue, final Object exclusiveToValue) {
        return graph.getEdgeIndexModel().edgesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Edge> edgesWithLimit(final Direction direction, final String label, final String key,
                                         final Object fromValue, final int limit) {
        return edgesWithLimit(direction, label, key, fromValue, limit, false);
    }

    public Iterator<Edge> edgesWithLimit(final Direction direction, final String label, final String key,
                                         final Object fromValue, final int limit, final boolean reversed) {
        return graph.getEdgeIndexModel().edgesWithLimit(this, direction, label, key, fromValue, limit, reversed);
    }

    public Iterator<Vertex> vertices(final Direction direction, final String label, final String key, final Object value) {
        return graph.getEdgeIndexModel().vertices(this, direction, label, key, value);
    }

    public Iterator<Vertex> verticesInRange(final Direction direction, final String label, final String key,
                                            final Object inclusiveFromValue, final Object exclusiveToValue) {
        return graph.getEdgeIndexModel().verticesInRange(this, direction, label, key, inclusiveFromValue, exclusiveToValue);
    }

    public Iterator<Vertex> verticesWithLimit(final Direction direction, final String label, final String key,
                                              final Object fromValue, final int limit) {
        return verticesWithLimit(direction, label, key, fromValue, limit, false);
    }

    public Iterator<Vertex> verticesWithLimit(final Direction direction, final String label, final String key,
                                              final Object fromValue, final int limit, final boolean reversed) {
        return graph.getEdgeIndexModel().verticesWithLimit(this, direction, label, key, fromValue, limit, reversed);
    }
*/
    //////////////////////////////////////////////////////////////////////////////////////

    @Override
    public String toString() { return "v[" + id() + "]"; }
}