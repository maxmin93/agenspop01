package net.bitnine.agenspop.graph.structure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public final class AgensHelper {

    private static final String NOT_FOUND_EXCEPTION = "NotFoundException";

    private AgensHelper() { }

    protected static Edge addEdge(final AgensGraph graph, final AgensVertex outVertex, final AgensVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }

        edge = new AgensEdge(idValue, outVertex, label, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id(), edge);
        AgensHelper.addOutEdge(outVertex, label, edge);
        AgensHelper.addInEdge(inVertex, label, edge);
        return edge;

    }

    protected static void addOutEdge(final AgensVertex vertex, final String label, final Edge edge) {
        if (null == vertex.outEdges) vertex.outEdges = new HashMap<>();
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final AgensVertex vertex, final String label, final Edge edge) {
        if (null == vertex.inEdges) vertex.inEdges = new HashMap<>();
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    public static List<AgensVertex> queryVertexIndex(final AgensGraph graph, final String key, final Object value) {
        return null == graph.vertexIndex ? Collections.emptyList() : graph.vertexIndex.get(key, value);
    }

    public static List<AgensEdge> queryEdgeIndex(final AgensGraph graph, final String key, final Object value) {
        return null == graph.edgeIndex ? Collections.emptyList() : graph.edgeIndex.get(key, value);
    }

    public static boolean inComputerMode(final AgensGraph graph) {
        return null != graph.graphComputerView;
    }

    public static Object createGraphComputerView(final AgensGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        return graph.graphComputerView = null;
    }

    public static Object getGraphComputerView(final AgensGraph graph) {
        return graph.graphComputerView;
    }

    public static void dropGraphComputerView(final AgensGraph graph) {
        graph.graphComputerView = null;
    }

    public static Map<String, List<VertexProperty>> getProperties(final AgensVertex vertex) {
        return null == vertex.properties ? Collections.emptyMap() : vertex.properties;
    }

    public static void autoUpdateIndex(final AgensEdge edge, final String key, final Object newValue, final Object oldValue) {
        final AgensGraph graph = (AgensGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.autoUpdate(key, newValue, oldValue, edge);
    }

    public static void autoUpdateIndex(final AgensVertex vertex, final String key, final Object newValue, final Object oldValue) {
        final AgensGraph graph = (AgensGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.autoUpdate(key, newValue, oldValue, vertex);
    }

    public static void removeElementIndex(final AgensVertex vertex) {
        final AgensGraph graph = (AgensGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.removeElement(vertex);
    }

    public static void removeElementIndex(final AgensEdge edge) {
        final AgensGraph graph = (AgensGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.removeElement(edge);
    }

    public static void removeIndex(final AgensVertex vertex, final String key, final Object value) {
        final AgensGraph graph = (AgensGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.remove(key, value, vertex);
    }

    public static void removeIndex(final AgensEdge edge, final String key, final Object value) {
        final AgensGraph graph = (AgensGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.remove(key, value, edge);
    }

    public static Iterator<AgensEdge> getEdges(final AgensVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(edges::addAll);
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
            }
        }
        return (Iterator) edges.iterator();
    }

    public static Iterator<AgensVertex> getVertices(final AgensVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((AgensEdge) edge).inVertex)));
                else if (edgeLabels.length == 1)
                    vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((AgensEdge) edge).inVertex));
                else
                    Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((AgensEdge) edge).inVertex));
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((AgensEdge) edge).outVertex)));
                else if (edgeLabels.length == 1)
                    vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((AgensEdge) edge).outVertex));
                else
                    Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((AgensEdge) edge).outVertex));
            }
        }
        return (Iterator) vertices.iterator();
    }

    public static Map<Object, Vertex> getVertices(final AgensGraph graph) {
        return graph.vertices;
    }

    public static Map<Object, Edge> getEdges(final AgensGraph graph) {
        return graph.edges;
    }

    //////////////////////////////////////////

    public static boolean isDeleted(final ElasticVertex vertex) {
        try {
            vertex.getKeys();
            return false;
        } catch (final RuntimeException e) {
            if (isNotFound(e))
                return true;
            else
                throw e;
        }
    }

    public static boolean isNotFound(final RuntimeException ex) {
        return ex.getClass().getSimpleName().equals(NOT_FOUND_EXCEPTION);
    }

    public static ElasticVertex getVertexPropertyNode(final AgensVertexProperty vertexProperty) {
        return (ElasticVertex)vertexProperty.vertexPropertyBase;
    }

    public static void setVertexPropertyNode(final AgensVertexProperty vertexProperty, final ElasticVertex vertex) {
        vertexProperty.vertexPropertyBase = vertex;
    }
}
