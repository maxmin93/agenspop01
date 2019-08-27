package net.bitnine.agenspop.graph.structure;

import java.util.*;
import java.util.stream.Collectors;

import net.bitnine.agenspop.basegraph.BaseGraphAPI;
import net.bitnine.agenspop.basegraph.model.BaseEdge;
import net.bitnine.agenspop.basegraph.model.BaseElement;
import net.bitnine.agenspop.basegraph.model.BaseProperty;
import net.bitnine.agenspop.basegraph.model.BaseVertex;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public final class AgensHelper {

    private static final String NOT_FOUND_EXCEPTION = "NotFoundException";

    private AgensHelper() { }

    public static void attachProperties(final BaseGraphAPI api, final BaseElement element, final Object... propertyKeyValues) {
        if (api == null) throw Graph.Exceptions.argumentCanNotBeNull("baseGraphAPI");
        if (element == null) throw Graph.Exceptions.argumentCanNotBeNull("baseElement");
        for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
            if (!propertyKeyValues[i].equals(T.id) && !propertyKeyValues[i].equals(T.label)){
                BaseProperty propertyBase = api.createProperty((String)propertyKeyValues[i], propertyKeyValues[i + 1]);
                element.setProperty(propertyBase);
            }
        }
    }
    public static void attachProperties(final Vertex vertex, final Object... propertyKeyValues) {
        if (vertex == null) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        BaseGraphAPI api = ((AgensVertex)vertex).graph.getBaseGraph();
        BaseElement baseElement = ((AgensVertex)vertex).baseElement;
        attachProperties(api, baseElement, propertyKeyValues);
    }
    public static void attachProperties(final Edge edge, final Object... propertyKeyValues) {
        if (edge == null) throw Graph.Exceptions.argumentCanNotBeNull("edge");
        BaseGraphAPI api = ((AgensEdge)edge).graph.getBaseGraph();
        BaseElement baseElement = ((AgensEdge)edge).baseElement;
        attachProperties(api, baseElement, propertyKeyValues);
    }

    protected static Vertex addVertex(final AgensGraph graph, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final Vertex vertex;    // if throw exception, then null

        Object idValue = graph.vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null != idValue) {
            if (graph.api.existsVertex(idValue.toString()))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.vertexIdManager.getNextId(graph);
        }

        graph.tx().readWrite();
        final BaseVertex baseVertex = graph.api.createVertex(graph.name(), idValue.toString(), label);
        AgensHelper.attachProperties(graph.api, baseVertex, keyValues);
        graph.api.saveVertex(baseVertex);     // write to elasticsearch index

        return new AgensVertex(graph, baseVertex);
    }

    protected static Edge addEdge(final AgensGraph graph, final AgensVertex outVertex, final AgensVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        final Edge edge;    // if throw exception, then null

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        if (null != idValue) {
            if (graph.api.existsEdge(idValue.toString()))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }

        graph.tx().readWrite();
        final BaseEdge baseEdge = graph.api.createEdge(graph.name(), idValue.toString(), label
                , outVertex.id().toString(), inVertex.id().toString());
        AgensHelper.attachProperties(graph.api, baseEdge, keyValues);
        graph.api.saveEdge(baseEdge);     // write to elasticsearch index

        return new AgensEdge(graph, baseEdge);
    }

    //////////////////////////////////////////

    public static boolean isDeleted(final BaseVertex vertex) {
        try {
            return vertex.removed();
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
    public static Map<String, Object> optimizeHasContainers(List<HasContainer> hasContainers){
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

    public static Collection<Vertex> verticesWithHasContainers(AgensGraph graph, Map<String, Object> optimizedParams) {
        String label = !optimizedParams.containsKey("label") ? null : optimizedParams.get("label").toString();
        List<String> labelParams = !optimizedParams.containsKey("labels") ? null : (List<String>) optimizedParams.get("labels");
        String key = !optimizedParams.containsKey("key") ? null : optimizedParams.get("key").toString();
        String keyNot = !optimizedParams.containsKey("keyNot") ? null : optimizedParams.get("keyNot").toString();
        List<String> keyParams = !optimizedParams.containsKey("keys") ? null : (List<String>) optimizedParams.get("keys");
        List<String> valueParams = !optimizedParams.containsKey("values") ? null : (List<String>) optimizedParams.get("values");
        Map<String, String> kvPairs = !optimizedParams.containsKey("kvPairs") ? null : (Map<String, String>) optimizedParams.get("kvPairs");

        // Parameters
        String[] labels = labelParams==null ? null : labelParams.stream().toArray(String[]::new);
        String[] keys = keyParams==null ? null : keyParams.stream().toArray(String[]::new);
        String[] values = valueParams==null ? null : valueParams.stream().toArray(String[]::new);

        // for DEBUG
        System.out.println("V.hasContainers :: datasource => "+graph.name());
        System.out.println("  , label => "+label);
        System.out.println("  , labels => "+(labels==null ? "null" : String.join(",", labels)));
        System.out.println("  , key => "+key);
        System.out.println("  , keyNot => "+keyNot);
        System.out.println("  , keys => "+(keys==null ? "null" : String.join(",", keys)));
        System.out.println("  , values => "+(values==null ? "null" : String.join(",", values)));
        System.out.println("  , kvPairs => "+(kvPairs==null ? "null" : kvPairs.entrySet().stream().map(r->r.getKey()+"="+r.getValue()).collect(Collectors.joining(","))));

        return graph.api.findVertices(graph.name()
                , label, labels, key, keyNot, keys, values, kvPairs).stream()
                .map(node -> (Vertex) new AgensVertex(graph, node)).collect(Collectors.toList());
    }


}
