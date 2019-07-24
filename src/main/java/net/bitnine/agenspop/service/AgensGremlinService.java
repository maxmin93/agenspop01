package net.bitnine.agenspop.service;

import com.google.common.base.Joiner;
import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.web.dto.DetachedGraph;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.opencypher.gremlin.translation.CypherAst;
import org.opencypher.gremlin.translation.TranslationFacade;
import org.opencypher.gremlin.translation.groovy.GroovyPredicate;
import org.opencypher.gremlin.translation.translator.Translator;
import org.opencypher.gremlin.translation.translator.TranslatorFlavor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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

    @PostConstruct
    private void ready() {
//        String script = "v1='modern_1'; v2='modern_2'; vlist = modern_g.V(v1,v2);";
//        String script = "modern_g.V().valueMap()";
//        String script = "modern_g.V().as('a').hasLabel('person').has('name',eq('vadas'))";
        String script = "modern_g.V().hasKey('age','name')";
//        String script = "modern_g.E().as('a').hasLabel('knows').project('a').by(__.identity()).limit(2).project('a').by(__.select('a').project('cypher.element','cypher.inv','cypher.outv').by(__.valueMap().with('~tinkerpop.valueMap.tokens')).by(__.inV().id()).by(__.outV().id()))";
        try {
            CompletableFuture<?> future = runGremlin(script);
            CompletableFuture.allOf(future).join();
            Object result = (Object)future.get();

            if( result != null  && result instanceof List) {
                List<Object> resultList = (List<Object>) future.get();
                if (resultList != null && resultList.size() > 0) {
                    if (resultList.get(0) instanceof LinkedHashMap) {
                        for (Object item : resultList) {
                            LinkedHashMap<String, Object> list = (LinkedHashMap<String, Object>) item;
                            list.forEach((k, v) -> System.out.println("  " + k + " -> " + v + "|" + v.getClass().getSimpleName()));
                        }
                    } else {
                        resultList.stream().forEach(r ->
                                System.out.println("  ==> " + r.toString() + "|" + r.getClass().getSimpleName()));
                    }
                }
            } else {
                System.out.println("  ==> " + result.toString() + "|" + result.getClass().getSimpleName());
            }
        }catch (Exception ex){
            System.out.println("** ERROR: runScript ==> " + ex.getMessage());
        }
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
                System.out.print("** traversal: \""+script+"\"\n  ==> "+t.toString()+" <"+t.hasNext()+">\n");

                List<Object> resultList = new ArrayList<>();
                while( t.hasNext() ) {          // if result exists,
                    resultList.add(t.next());
                }
                return CompletableFuture.completedFuture(resultList);
            }
            // for DEBUG
            System.out.print("  ==> "+result.toString()+"|"+result.getClass().getSimpleName());
            return CompletableFuture.completedFuture(result);

        } catch (Exception ex) {
            // tossed to exceptionCaught which delegates to sendError method
            final Throwable t = ExceptionUtils.getRootCause(ex);
            throw new RuntimeException(null == t ? ex : t);
        }
    }


    @Async("agensExecutor")
    public CompletableFuture<?> runCypher(String cypher, String datasource){
        try{
            // **참고
            // https://github.com/opencypher/cypher-for-gremlin/tree/master/translation
            //
            // translate cypher query to gremlin
            String script = cfog.toGremlinGroovy(cypher);
/*
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
            System.out.println("** translate: "+cypher+" ==> "+script);

            // use no timeout on the engine initialization - perhaps this can be a configuration later
            final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build().
                    scriptEvaluationTimeoutOverride(0L).create();
            final Bindings bindings = new SimpleBindings(Collections.emptyMap());

            final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, GREMLIN_GROOVY, bindings, lifeCycle);
            CompletableFuture.allOf(evalFuture).join();

            Object result = evalFuture.get();
            if( result != null && result instanceof DefaultGraphTraversal ){
                DefaultGraphTraversal t = (DefaultGraphTraversal) evalFuture.get();
                // for DEBUG
                System.out.println("** traversal: \""+script+"\" ==> "+t.toString());

                if( t.hasNext() ){          // if result exists,
                    Object r = t.next();
                    if( t.hasNext() ){      // if result is iterable,
                        List<Object> resultList = new ArrayList<>();
                        resultList.add(r);
                        while( t.hasNext() ) resultList.add(t.next());

                        return CompletableFuture.completedFuture(resultList);
                    }
                    return CompletableFuture.completedFuture(r);
                }
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(result);

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
    public CompletableFuture<List<AgensVertex>> getVertices(String gName, String... labels) throws InterruptedException {
        if( !graphManager.getGraphNames().contains(gName) )
            return CompletableFuture.completedFuture( null );

        GraphTraversalSource ts = (GraphTraversalSource) graphManager.getTraversalSource(GRAPH_TRAVERSAL_NAME.apply(gName));
        if( ts == null ) return CompletableFuture.completedFuture( null );

        GraphTraversal t;
        if( labels.length == 0 ) t = ts.V();
        // **NOTE: 최적화 필요
        // ex) AgensGraphStep(vertex,[~label.within([customer, order])])
        else t = ts.V().hasLabel(labels[0], Arrays.copyOfRange(labels, 1, labels.length));

        List<AgensVertex> vertices = new ArrayList<>();
        while( t.hasNext() ) vertices.add( (AgensVertex) t.next() );

        return CompletableFuture.completedFuture( vertices );
    }

    @Async("agensExecutor")
    public CompletableFuture<List<AgensEdge>> getEdges(String gName, String... labels) throws InterruptedException {
        if( !graphManager.getGraphNames().contains(gName) )
            return CompletableFuture.completedFuture( null );

        GraphTraversalSource ts = (GraphTraversalSource) graphManager.getTraversalSource(GRAPH_TRAVERSAL_NAME.apply(gName));
        if( ts == null ) return CompletableFuture.completedFuture( null );

        GraphTraversal t;
        if( labels.length == 0 ) t = ts.E();
        // **NOTE: fetch size = 100 관련 중요 문제
        // contains 데이터가 많아, sold/part_of 등의 소수의 label 들을 쿼리하면 나오지 않는다
        // ==> V().hasLabel(), E().hasLabel() Step을 ES 쿼리에 적합하도록 최적화해야 함!!
        // ==> 관련 클래스 : AgensGraphStepStrategy, AgensLabelStep (신규)
        else t = ts.E().hasLabel(labels[0], Arrays.copyOfRange(labels, 1, labels.length));

        List<AgensEdge> edges = new ArrayList<>();
        while( t.hasNext() ) edges.add( (AgensEdge) t.next() );

        return CompletableFuture.completedFuture( edges );
    }

}
