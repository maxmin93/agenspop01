package net.bitnine.agenspop.graph.structure;


import net.bitnine.agenspop.elastic.ElasticGraphAPI;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols.otherV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols.values;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.gryo;

/**
 * Helps create a variety of different toy graphs for testing and learning purposes.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class AgensFactory {

    public static final AtomicInteger graphSeq = new AtomicInteger(0);
    public static final String GREMLIN_DEFAULT_GRAPH_NAME = "default";

    public static String defaultGraphName(){
        return GREMLIN_DEFAULT_GRAPH_NAME+graphSeq.incrementAndGet();
    }

    private static Configuration getMixIdManagerConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_GRAPH_NAME, defaultGraphName());
        // 필요 없음. AgensGraph 초기화시 IdManager 고정
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_VERTEX_ID_MANAGER, AgensIdManager.MIX_ID.name());
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_EDGE_ID_MANAGER, AgensIdManager.MIX_ID.name());
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.single);
        return conf;
    }

    private AgensFactory() {}

    /////////////////////////////////////

    public static AgensGraph createEmpty(ElasticGraphAPI baseGraph, String gName) {
        final Configuration conf = getMixIdManagerConfiguration();
        conf.setProperty(AgensGraph.GREMLIN_AGENSGRAPH_GRAPH_NAME, gName);
        return AgensGraph.open(baseGraph, conf);
    }

    /**
     * Create the "modern" graph which has the same structure as the "classic" graph from AgensPop 2.x but includes
     * 3.x features like vertex labels.
     */
    public static AgensGraph createModern(ElasticGraphAPI baseGraph) {
        final Configuration conf = getMixIdManagerConfiguration();
        final AgensGraph g = AgensGraph.open(baseGraph, conf);
        generateModern(g);
        return g;
    }

    public static void generateModern(final AgensGraph g) {
        final Vertex marko = g.addVertex(T.id, 1, T.label, "person", "name", "marko", "age", 29, "country", "USA");
        final Vertex vadas = g.addVertex(T.id, 2, T.label, "person", "name", "vadas", "age", 27, "country", "USA");
        final Vertex lop = g.addVertex(T.id, 3, T.label, "software", "name", "lop", "lang", "java");
        final Vertex josh = g.addVertex(T.id, 4, T.label, "person", "name", "josh", "age", 32, "country", "USA");
        final Vertex ripple = g.addVertex(T.id, 5, T.label, "software", "name", "ripple", "lang", "java");
        final Vertex peter = g.addVertex(T.id, 6, T.label, "person", "name", "peter", "age", 35, "country", "USA");
        marko.addEdge("knows", vadas, T.id, 7, "weight", 0.5d);
        marko.addEdge("knows", josh, T.id, 8, "weight", 1.0d);
        marko.addEdge("created", lop, T.id, 9, "weight", 0.4d);
        josh.addEdge("created", ripple, T.id, 10, "weight", 1.0d);
        josh.addEdge("created", lop, T.id, 11, "weight", 0.4d);
        peter.addEdge("created", lop, T.id, 12, "weight", 0.2d);

        // remove test ==> vertex{1}, edge{7,8,9}
        System.out.println("  - before remove V(marko): "+g.toString());
        marko.remove();

/*
        GraphTraversalSource t = g.traversal();
        List<Vertex> vertexList = t.V().next(100);
        System.out.println("  - list vertices ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));
        List<Edge> edgeList = t.E().next(100);
        System.out.println("  - list edges ==> "+edgeList.stream().map(Edge::toString).collect(Collectors.joining(",")));
        vertexList = t.V("modern::5", "modern::4", "modern::3").next(100);
        System.out.println("  - vertex ids ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));
        edgeList = t.V("modern::2").bothE().next(100);
        System.out.println("  - edge bothE ==> "+edgeList.stream().map(Edge::toString).collect(Collectors.joining(",")));
        Vertex v1 = t.V("modern::1").next();
        vertexList = t.V(v1).out().next(100);
        System.out.println("  - vertex(1) out neighbors ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));

        List<Object> valueList = t.V().values("name").next(100);
        System.out.println("  - vertices.value('name') ==> "+valueList.stream().map(v->String.valueOf(v)).collect(Collectors.joining(",")));

        vertexList = t.V().has("name","josh").next(100);
        System.out.println("  - vertex has ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));
        vertexList = t.V().hasLabel("person").out("knows").where(__.values("age").is(P.lt(30))).next(100);
        System.out.println("  - vertex where ==> "+vertexList.stream().map(Vertex::toString).collect(Collectors.joining(",")));
 */
    }
}