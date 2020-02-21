package net.bitnine.agenspop.service;

import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import net.bitnine.agenspop.graph.structure.AgensHelper;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.dto.DetachedGraph;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.opencypher.gremlin.translation.TranslationFacade;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static net.bitnine.agenspop.graph.AgensGraphManager.GRAPH_TRAVERSAL_NAME;

@Service
public class AgensGremlinService {

    public static enum SCRIPT_TYPE { GREMLIN, CYPHER };
    public final static String GREMLIN_GROOVY = "gremlin-groovy";

    private final AgensGraphManager graphManager;
    private final AgensGroovyServer groovyServer;
    private final GremlinExecutor gremlinExecutor;
    private final TranslationFacade cfog;

    @Autowired
    AgensGremlinService(
            AgensGraphManager graphManager,
            AgensGroovyServer groovyServer
    ){
        this.graphManager = graphManager;
        this.groovyServer = groovyServer;
        this.gremlinExecutor = groovyServer.getGremlinExecutor();
        this.cfog = new TranslationFacade();
    }

    @Async("agensExecutor")
    public CompletableFuture<?> runGremlin(String script){
        try{
            // use no timeout on the engine initialization - perhaps this can be a configuration later
            final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build().
                    scriptEvaluationTimeoutOverride(0L).create();
            final Bindings bindings = new SimpleBindings(Collections.emptyMap());

            final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, GREMLIN_GROOVY, bindings, lifeCycle);
            CompletableFuture.allOf(evalFuture).join();

            Object result = evalFuture.get();
            if( result != null && result instanceof GraphTraversal ){
                GraphTraversal t = (GraphTraversal) evalFuture.get();
                // for DEBUG
                System.out.println("**traversal: \""+script+"\"\n  ==> "+t.toString()+" <"+t.hasNext()+">\n");
//                List<Object> resultList = new ArrayList<>();
//                while( t.hasNext() ) { resultList.add(t.next()); }

                return CompletableFuture.completedFuture(
                        AgensHelper.getStreamFromIterator((Iterator<Object>)t) );
            }
            // for DEBUG
            System.out.println("  ==> "+result.toString()+"|"+result.getClass().getSimpleName()+"\n");
            return CompletableFuture.completedFuture( Stream.of(result) );

        } catch (Exception ex) {
            // tossed to exceptionCaught which delegates to sendError method
            final Throwable t = ExceptionUtils.getRootCause(ex);
            throw new RuntimeException(null == t ? ex : t);
        }
    }

/*
String script = "modern_g.E().as('a').project('a').by(__.identity()).limit(2).project('a').by(__.select('a').project('cypher.element', 'cypher.inv', 'cypher.outv').by(__.valueMap().with('~tinkerpop.valueMap.tokens')).by(__.inV().id()).by(__.outV().id()))";
expected> type = LinkedHashMap()
==>[a:[cypher.element:[id:7,label:knows,weight:0.5],cypher.inv:2,cypher.outv:1]]
==>[a:[cypher.element:[id:8,label:knows,weight:1.0],cypher.inv:4,cypher.outv:1]]

==> modern_g.E().valueMap()
** NOTE:
  Cypher-for-Gremlin 변환기를 거치면 결과를 LinkedHashMap 형태로 뱉는다!!
  ==> {country=[USA], age=[29], name=[marko]}   |LinkedHashMap
  ==> {country=[USA], age=[27], name=[vadas]}   |LinkedHashMap
  ==> {lang=[java], name=[lop]}                 |LinkedHashMap
  ==> {country=[USA], age=[32], name=[josh]}    |LinkedHashMap
  ==> {lang=[java], name=[ripple]}              |LinkedHashMap
  ==> {country=[USA], age=[35], name=[peter]}   |LinkedHashMap
 */

    @Async("agensExecutor")
    public CompletableFuture<?> runCypher(String cypher, String datasource){
        try{
            // **참고
            // https://github.com/opencypher/cypher-for-gremlin/tree/master/translation
            //
            // translate cypher query to gremlin
            String script = cfog.toGremlinGroovy(cypher);

/*
            // **NOTE: 기본 translator 와 별 다를게 없다
            String cypher = "MATCH (p:Person) WHERE p.age > 25 RETURN p.name";
            CypherAst ast = CypherAst.parse(cypher);
            Translator<String, GroovyPredicate> translator = Translator.builder().gremlinGroovy().build(TranslatorFlavor.cosmosDb());
            String script = ast.buildTranslation(translator);
*/
            // replace to graph traversal of datasource
            if( script.length() > 2 && script.startsWith("g.") ) {
                script = AgensGraphManager.GRAPH_TRAVERSAL_NAME.apply(datasource)
                        + "." + script.substring(2);
                script = script.replaceAll("\\s+","");   // remove tailling spaces
            }
            // for DEBUG
            System.out.println("**cypher-to-gremlin: "+cypher+"\n  ==> "+script);

            // use no timeout on the engine initialization - perhaps this can be a configuration later
            final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build().
                    scriptEvaluationTimeoutOverride(0L).create();
            final Bindings bindings = new SimpleBindings(Collections.emptyMap());

            final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, GREMLIN_GROOVY, bindings, lifeCycle);
            CompletableFuture.allOf(evalFuture).join();

            Object result = evalFuture.get();
            if( result != null && result instanceof DefaultGraphTraversal ){    // DefaultGraphTraversal
                // DefaultGraphTraversal t = (DefaultGraphTraversal) evalFuture.get();
                DefaultGraphTraversal t = (DefaultGraphTraversal) evalFuture.get();
                // for DEBUG
                System.out.println("**traversal: \""+script+"\"\n  ==> "+t.toString()+" <"+t.hasNext()+">\n");
//                List<Object> resultList = new ArrayList<>();
//                while( t.hasNext() ) { resultList.add(t.next()); }

                return CompletableFuture.completedFuture(
                        AgensHelper.getStreamFromIterator((Iterator<Object>)t) );
            }
            return CompletableFuture.completedFuture(Stream.of(result));

        } catch (Exception ex) {
            // tossed to exceptionCaught which delegates to sendError method
            final Throwable t = ExceptionUtils.getRootCause(ex);
            throw new RuntimeException(null == t ? ex : t);
        }
    }

    ////////////////////////////////////////////

    @Async("agensExecutor")
    public CompletableFuture<DetachedGraph> getGraph(String gName) throws InterruptedException {
        if( !graphManager.getGraphNames().contains(gName) )
            return CompletableFuture.completedFuture( null );

        GraphTraversalSource ts = (GraphTraversalSource) graphManager.getTraversalSource(GRAPH_TRAVERSAL_NAME.apply(gName));
        if( ts == null ) return CompletableFuture.completedFuture( null );

        List<AgensVertex> vertices = ts.V().next(100).stream()
                .filter(c->c instanceof AgensVertex).map(v->(AgensVertex)v)
                .collect(Collectors.toList());
        List<AgensEdge> edges = ts.E().next(100).stream()
                .filter(c->c instanceof AgensEdge).map(e->(AgensEdge)e)
                .collect(Collectors.toList());

        DetachedGraph graph = new DetachedGraph(gName, vertices, edges);
        return CompletableFuture.completedFuture( graph );
    }

    @Async("agensExecutor")
    public CompletableFuture<List<AgensVertex>> getVertices(String gName
            , String label, List<String> labels
            , String key, String keyNot, List<String> keys
            , List<String> values, Map<String, String> kvPairs
    ) throws InterruptedException {
        if( !graphManager.getGraphNames().contains(gName) )
            return CompletableFuture.completedFuture( Collections.EMPTY_LIST );

        AgensGraph g = (AgensGraph) graphManager.getGraph(gName);
        Map<String, Object> params = AgensHelper.optimizedParams(label, labels, key, keyNot, keys, values, kvPairs);
        Iterator<Vertex> t =  params.size() == 0 ? g.vertices()
                : AgensHelper.verticesWithHasContainers(g, params).iterator();

        List<AgensVertex> vertices = new ArrayList<>();
        while( t.hasNext() ) vertices.add( (AgensVertex) t.next() );

        return CompletableFuture.completedFuture( vertices );
    }

    @Async("agensExecutor")
    public CompletableFuture<List<AgensEdge>> getEdges(String gName
            , String label, List<String> labels
            , String key, String keyNot, List<String> keys
            , List<String> values, Map<String, String> kvPairs
    ) throws InterruptedException {
        if( !graphManager.getGraphNames().contains(gName) )
            return CompletableFuture.completedFuture( Collections.EMPTY_LIST );

        AgensGraph g = (AgensGraph) graphManager.getGraph(gName);
        Map<String, Object> params = AgensHelper.optimizedParams(label, labels, key, keyNot, keys, values, kvPairs);
        Iterator<Edge> t = params.size() == 0 ? g.edges()
                : AgensHelper.edgesWithHasContainers(g, params).iterator();

        List<AgensEdge> edges = new ArrayList<>();
        while( t.hasNext() ) edges.add( (AgensEdge) t.next() );

        return CompletableFuture.completedFuture( edges );
    }

}
