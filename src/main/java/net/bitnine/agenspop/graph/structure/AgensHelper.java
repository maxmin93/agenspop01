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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import net.bitnine.agenspop.elastic.document.ElasticPropertyDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public final class AgensHelper {

    private static final String NOT_FOUND_EXCEPTION = "NotFoundException";

    private AgensHelper() { }

    public static void attachProperties(final Vertex vertex, final Object... propertyKeyValues) {
        if (vertex == null) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label)){
                ((AgensVertex)vertex).property((String)propertyKeyValues[i], propertyKeyValues[i + 1]);
            }
        }
    }
    public static void attachProperties(final Edge edge, final Object... propertyKeyValues) {
        if (edge == null) throw Graph.Exceptions.argumentCanNotBeNull("edge");
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label))
                ((AgensEdge)edge).property((String) propertyKeyValues[i], propertyKeyValues[i + 1]);
        }
    }

    protected static Edge addEdge(final AgensGraph graph, final AgensVertex outVertex, final AgensVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final Edge edge;    // if throw exception, then null

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null), graph);
        if (null != idValue) {
            if (graph.baseGraph.existsEdge(idValue.toString()))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }

        graph.tx().readWrite();
        final ElasticEdge elasticEdge = graph.baseGraph.createEdge(
                idValue.toString(), label, outVertex.id().toString(), inVertex.id().toString()
        );
        edge = new AgensEdge(elasticEdge, graph);
        ElementHelper.attachProperties(edge, keyValues);
        graph.baseGraph.saveEdge(elasticEdge);     // write to elasticsearch index

        return edge;
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

    public static Map<String, VertexProperty> getProperties(final AgensVertex vertex) {
        return null == vertex.properties ? Collections.emptyMap() : vertex.properties;
    }

    //////////////////////////////////////////

    public static boolean isDeleted(final ElasticVertex vertex) {
        try {
            vertex.getKeys();
            return vertex.isDeleted();
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

}
