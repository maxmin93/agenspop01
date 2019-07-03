package net.bitnine.agenspop.graph;

import net.bitnine.agenspop.graph.structure.*;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.File;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

public abstract class AbstractAgensGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(AgensEdge.class);
        add(AgensElement.class);
        add(AgensGraph.class);
        add(AgensGraphVariables.class);
        add(AgensProperty.class);
        add(AgensVertex.class);
        add(AgensVertexProperty.class);
    }};

    protected Graph.Features features = null;

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        final Graph graph = super.openTestGraph(config);

        // we can just use the initial set of features taken from the first graph generated from the provider because
        // neo4j feature won't ever change. don't think there is any danger of keeping this instance about even if
        // the original graph instance goes out of scope.
        if (null == features) {
            this.features = graph.features();
        }
        return graph;
    }

    @Override
    public Optional<Graph.Features> getStaticFeatures() {
        return Optional.ofNullable(features);
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (null != graph) {
            if (graph.tx().isOpen()) graph.tx().rollback();
            graph.close();
        }

        if (null != configuration && configuration.containsKey(AgensGraph.CONFIG_DIRECTORY)) {
            // this is a non-in-sideEffects configuration so blow away the directory
            final File graphDirectory = new File(configuration.getString(AgensGraph.CONFIG_DIRECTORY));
            deleteDirectory(graphDirectory);
        }
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (loadGraphWith != null) this.createIndices((AgensGraph) graph, loadGraphWith.value());
        super.loadGraphData(graph, loadGraphWith, testClass, testName);
    }

}
