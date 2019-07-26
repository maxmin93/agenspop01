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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.bitnine.agenspop.elastic.document.ElasticPropertyDocument;
import net.bitnine.agenspop.elastic.model.ElasticEdge;
import net.bitnine.agenspop.elastic.model.ElasticElement;
import net.bitnine.agenspop.elastic.model.ElasticProperty;
import net.bitnine.agenspop.elastic.model.ElasticVertex;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
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

    // **NOTE: 최적화된 hasContainer 는 삭제!!
    //      ==> hasContainer.test(element) 에서 실패 방지
    public static int optimizeHasContainers(List<HasContainer> hasContainers,
                       List<String> ids, List<String> labels, List<String> keys, List<Object> values){
        int optType = 0;
        Iterator<HasContainer> iter = hasContainers.iterator();
        while( iter.hasNext() ){
            HasContainer c = iter.next();
            // hasId(id...)
            if( c.getKey().equals("~id") ){
                if( c.getBiPredicate().toString().equals("eq") ){
                    ids.add( (String)c.getValue() );
                    optType += 100000;
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    ids.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optType += 100000*valueList.size();
                    iter.remove();      // remove hasContainer!!
                }
                // return optType;      // skips other hasContainers
            }
            // hasLabel(label...)
            else if( c.getKey().equals("~label") ){
                if( c.getBiPredicate().toString().equals("eq") ){
                    labels.add( (String)c.getValue() );
                    optType += 10000;
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    labels.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optType += 10000*valueList.size();
                    iter.remove();      // remove hasContainer!!
                }
            }
            // hasKey(key...)
            else if( c.getKey().equals("~key") ){
                if( c.getBiPredicate().toString().equals("eq") ){
                    keys.add( c.getValue().toString() );
                    optType += 1000;
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    keys.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optType += 1000*valueList.size();
                    iter.remove();      // remove hasContainer!!
                }
            }
            // hasValue(value...)
            else if( c.getKey().equals("~value") ){
                if( c.getBiPredicate().toString().equals("eq") ){
                    values.add( c.getValue() );
                    optType += 100;
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    values.addAll( valueList.stream().map(Object::toString).collect(Collectors.toList()) );
                    optType += 100*valueList.size();
                    iter.remove();      // remove hasContainer!!
                }
            }
            // has(property
            else {
                if( c.getKey() != null ) keys.add(c.getKey());

                if( c.getBiPredicate().toString().equals("eq") ){
                    values.add( c.getValue() );
                    optType += 1;
                    iter.remove();      // remove hasContainer!!
                }
                else if( c.getBiPredicate().toString().equals("within") ){
                    List<Object> valueList = (List<Object>)c.getValue();
                    values.addAll( valueList );
                    optType += valueList.size();
                    iter.remove();      // remove hasContainer!!
                }
            }
        }
        return optType;
    }
}
