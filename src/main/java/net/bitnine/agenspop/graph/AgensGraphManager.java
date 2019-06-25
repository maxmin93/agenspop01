package net.bitnine.agenspop.graph;

import net.bitnine.agenspop.elastic.ElasticGraphAPI;
import net.bitnine.agenspop.elastic.ElasticGraphService;
import net.bitnine.agenspop.graph.exception.AgensGraphManagerException;
import net.bitnine.agenspop.graph.structure.AgensFactory;

import net.bitnine.agenspop.graph.structure.AgensGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;

import java.util.concurrent.ConcurrentHashMap;

import java.util.function.Function;
import java.util.Set;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.script.SimpleBindings;
import javax.script.Bindings;

@Service
public class AgensGraphManager implements GraphManager {

    private static final Logger log =
            LoggerFactory.getLogger(AgensGraphManager.class);
    public static final String AGENS_GRAPH_MANAGER_EXPECTED_STATE_MSG
            = "Gremlin Server must be configured to use the AgensGraphManager.";
    public static final Function<String, String> GRAPH_TRAVERSAL_NAME = (String gName) -> gName + "_traversal";

    private final Map<String, Graph> graphs = new ConcurrentHashMap<>();
    private final Map<String, TraversalSource> traversalSources = new ConcurrentHashMap<>();
    private final Object instantiateGraphLock = new Object();
    private GremlinExecutor gremlinExecutor = null;

    private final ElasticGraphAPI baseGraph;
    private static AgensGraphManager instance = null;

    /**
     * This class adheres to the TinkerPop graphManager specifications. It provides a coordinated
     * mechanism by which to instantiate graph references on a given AgensGraph node and a graph
     * reference tracker (or graph cache). Any graph created using the property \"graph.graphname\" and
     * any graph defined at server start, i.e. in the server's YAML file, will go through this
     * AgensGraphManager.
     */
    @Autowired
    public AgensGraphManager(ElasticGraphService baseGraph) {
        this.baseGraph = baseGraph;
        initialize();
    }

    private synchronized void initialize() {
        if (null != instance) {
            final String errMsg = "You may not instantiate a AgensGraphManager. The single instance should be handled by Tinkerpop's GremlinServer startup processes.";
            throw new AgensGraphManagerException(errMsg);
        }
        instance = this;
        // for DEBUG
        System.out.println("AgensGraphManager is initializing.");
    }

    @PostConstruct
    private synchronized void ready(){
        String gName = "modern";
        AgensGraph g = AgensFactory.createEmpty(baseGraph, gName);
        AgensFactory.generateModern(g);
        putGraph(gName, g);
        updateTraversalSource(gName, g);
        // for DEBUG
        System.out.println("AgensGraphManager ready: "+g.toString());

        // for Verify
        // http://localhost:9200/elasticvertex/_search?pretty=true&q=*:*
    }

    public static AgensGraphManager getInstance() {
        return instance;
    }

/*
    public void configureGremlinExecutor(GremlinExecutor gremlinExecutor) {
        this.gremlinExecutor = gremlinExecutor;
        final ScheduledExecutorService bindExecutor = Executors.newScheduledThreadPool(1);
        // Dynamically created graphs created with the ConfiguredGraphFactory are
        // bound across all nodes in the cluster and in the face of server restarts
        bindExecutor.scheduleWithFixedDelay(new GremlinExecutorGraphBinder(this, this.gremlinExecutor), 0, 20L, TimeUnit.SECONDS);
    }

    private class GremlinExecutorGraphBinder implements Runnable {
        final AgensGraphManager graphManager;
        final GremlinExecutor gremlinExecutor;

        public GremlinExecutorGraphBinder(AgensGraphManager graphManager, GremlinExecutor gremlinExecutor) {
            this.graphManager = graphManager;
            this.gremlinExecutor = gremlinExecutor;
        }

        @Override
        public void run() {
//            final Graph graph = ConfiguredGraphFactory.open(it);
//            updateTraversalSource(it, graph, this.gremlinExecutor, this.graphManager);
        }
    }
*/
    // To be used for testing purposes
    protected static void shutdownAgensGraphManager() {
        instance = null;
    }

    @Override
    public Set<String> getGraphNames() {
        return graphs.keySet();
    }

    @Override
    public Graph getGraph(String gName) {
        return graphs.get(gName);
    }

    @Override
    public void putGraph(String gName, Graph g) {
        graphs.put(gName, g);
    }

    @Override
    public Set<String> getTraversalSourceNames() {
        return traversalSources.keySet();
    }

    @Override
    public TraversalSource getTraversalSource(String tsName) {
        return traversalSources.get(tsName);
    }

    @Override
    public void putTraversalSource(String tsName, TraversalSource ts) {
        traversalSources.put(tsName, ts);
    }

    @Override
    public TraversalSource removeTraversalSource(String tsName) {
        if (tsName == null) return null;
        return traversalSources.remove(tsName);
    }

    /**
     * Get the {@link Graph} and {@link TraversalSource} list as a set of bindings.
     */
    @Override
    public Bindings getAsBindings() {
        final Bindings bindings = new SimpleBindings();
        graphs.forEach(bindings::put);
        traversalSources.forEach(bindings::put);
        return bindings;
    }

    @Override
    public void rollbackAll() {
        graphs.forEach((key, graph) -> {
            if (graph.tx().isOpen()) {
                graph.tx().rollback();
            }
        });
    }

    @Override
    public void rollback(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, false);
    }

    @Override
    public void commitAll() {
        graphs.forEach((key, graph) -> {
            if (graph.tx().isOpen())
                graph.tx().commit();
        });
    }

    @Override
    public void commit(final Set<String> graphSourceNamesToCloseTxOn) {
        commitOrRollback(graphSourceNamesToCloseTxOn, true);
    }

    public void commitOrRollback(Set<String> graphSourceNamesToCloseTxOn, Boolean commit) {
        graphSourceNamesToCloseTxOn.forEach(e -> {
            final Graph graph = getGraph(e);
            if (null != graph) {
                closeTx(graph, commit);
            }
        });
    }

    public void closeTx(Graph graph, Boolean commit) {
        if (graph.tx().isOpen()) {
            if (commit) {
                graph.tx().commit();
            } else {
                graph.tx().rollback();
            }
        }
    }

    @Override
    public Graph openGraph(String gName, Function<String, Graph> thunk) {
        Graph graph = graphs.get(gName);
        if (graph != null) {
            updateTraversalSource(gName, graph);
            return graph;
        } else {
            synchronized (instantiateGraphLock) {
                graph = graphs.get(gName);
                if (graph == null) {
                    graph = thunk.apply(gName);
                    graphs.put(gName, graph);
                }
            }
            updateTraversalSource(gName, graph);
            return graph;
        }
    }

    @Override
    public Graph removeGraph(String gName) {
        if (gName == null) return null;
        return graphs.remove(gName);
    }

    private void updateTraversalSource(String graphName, Graph graph){
        String traversalName = GRAPH_TRAVERSAL_NAME.apply(graphName);
        TraversalSource traversalSource = graph.traversal();
        putTraversalSource(traversalName, traversalSource);
//        if (null != gremlinExecutor) {
//            updateTraversalSource(graphName, graph, gremlinExecutor, this);
//        }
    }

/*
    private void updateTraversalSource(String graphName, Graph graph, GremlinExecutor gremlinExecutor,
                                       AgensGraphManager graphManager){
        gremlinExecutor.getScriptEngineManager().put(graphName, graph);
        String traversalName = graphName + "_traversal";
        TraversalSource traversalSource = graph.traversal();
        gremlinExecutor.getScriptEngineManager().put(traversalName, traversalSource);
        graphManager.putTraversalSource(traversalName, traversalSource);
    }
*/
}