package net.bitnine.agenspop.graph;

import net.bitnine.agenspop.elastic.ElasticGraphAPI;
import net.bitnine.agenspop.graph.structure.AgensGraph;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import static org.junit.Assert.assertEquals;

public class AbstractAgensGremlinTest extends AbstractGremlinTest {

    protected AgensGraph getGraph() {
        return (AgensGraph) this.graph;
    }

    protected ElasticGraphAPI getBaseGraph() {
        return ((AgensGraph) this.graph).getBaseGraph();
    }

    protected void validateCounts(int gV, int gE, int gN, int gR) {
        String datasource = ((AgensGraph)this.graph).name();
        assertEquals(gV, IteratorUtils.count(graph.vertices()));
        assertEquals(gE, IteratorUtils.count(graph.edges()));
        assertEquals(gN, IteratorUtils.count(this.getBaseGraph().findVertices(datasource)));
        assertEquals(gR, IteratorUtils.count(this.getBaseGraph().findEdges(datasource)));
    }
}
