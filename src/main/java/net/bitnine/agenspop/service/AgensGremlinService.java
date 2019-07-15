package net.bitnine.agenspop.service;

import net.bitnine.agenspop.graph.AgensGraphManager;
import net.bitnine.agenspop.graph.structure.AgensEdge;
import net.bitnine.agenspop.graph.structure.AgensVertex;
import net.bitnine.agenspop.web.dto.DetachedGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static net.bitnine.agenspop.graph.AgensGraphManager.GRAPH_TRAVERSAL_NAME;

@Service
public class AgensGremlinService {

    public final static int NEXT_FETCH_SIZE = 100;
    private final AgensGraphManager graphManager;

    @Autowired
    AgensGremlinService(AgensGraphManager graphManager ){
        this.graphManager = graphManager;
    }

    @Async("agensExecutor")
    public CompletableFuture<DetachedGraph> getGraph(String gName) throws InterruptedException {
        if( !graphManager.getGraphNames().contains(gName) )
            return CompletableFuture.completedFuture( null );

        GraphTraversalSource ts = (GraphTraversalSource) graphManager.getTraversalSource(GRAPH_TRAVERSAL_NAME.apply(gName));
        if( ts == null ) return CompletableFuture.completedFuture( null );

        List<AgensVertex> vertices = ts.V().next(NEXT_FETCH_SIZE).stream()
                .filter(c->c instanceof AgensVertex).map(v->(AgensVertex)v)
                .collect(Collectors.toList());
        List<AgensEdge> edges = ts.E().next(NEXT_FETCH_SIZE).stream()
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
