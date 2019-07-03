package net.bitnine.agenspop.graph;

import org.apache.tinkerpop.gremlin.FeatureRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NativeAgensStructureCheck extends AbstractAgensGremlinTest {

    @Test
    public void shouldOpenWithOverriddenConfig() throws Exception {
        assertNotNull(this.graph);
    }

    @Test
    public void shouldSupportHasContainersWithMultiLabels() throws Exception {
        final AgensVertex vertex = (AgensVertex) this.graph.addVertex(T.label, "person", "name", "marko");
        graph.tx().commit();
        assertTrue(g.V().has(T.label, "person").hasNext());
        assertEquals("marko", g.V().has(T.label, LabelP.of("person")).values("name").next());
        assertEquals("marko", g.V().has(T.label, "person").values("name").next());
        // more labels
        vertex.addLabel("animal");
        vertex.addLabel("object");
        graph.tx().commit();
        // no indices (neo4j graph step)
        assertFalse(g.V().has(T.label, "person").hasNext());
        assertEquals("marko", g.V().has(T.label, LabelP.of("person")).values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("animal")).values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("object")).values("name").next());
        // no indices (has step)
        assertFalse(g.V().as("a").select("a").has(T.label, "person").hasNext());
        assertEquals("marko", g.V().as("a").select("a").has(T.label, LabelP.of("person")).values("name").next());
        assertEquals("marko", g.V().as("a").select("a").has(T.label, LabelP.of("animal")).values("name").next());
        assertEquals("marko", g.V().as("a").select("a").has(T.label, LabelP.of("object")).values("name").next());
        // indices (neo4j graph step)
        this.getGraph().cypher("CREATE INDEX ON :person(name)").iterate();
        graph.tx().commit();
        Thread.sleep(500);
        assertFalse(g.V().has(T.label, "person").has("name", "marko").hasNext());
        assertEquals("marko", g.V().has(T.label, LabelP.of("person")).has("name", "marko").values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("animal")).has("name", "marko").values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("object")).has("name", "marko").values("name").next());
        this.getGraph().cypher("CREATE INDEX ON :animal(name)").iterate();
        graph.tx().commit();
        Thread.sleep(500);
        assertFalse(g.V().has(T.label, "animal").has("name", "marko").hasNext());
        assertEquals("marko", g.V().has(T.label, LabelP.of("person")).has("name", "marko").values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("animal")).has("name", "marko").values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("object")).has("name", "marko").values("name").next());
        this.getGraph().cypher("CREATE INDEX ON :object(name)").iterate();
        graph.tx().commit();
        Thread.sleep(500);
        assertFalse(g.V().has(T.label, "object").has("name", "marko").hasNext());
        assertEquals("marko", g.V().has(T.label, LabelP.of("person")).has("name", "marko").values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("animal")).has("name", "marko").values("name").next());
        assertEquals("marko", g.V().has(T.label, LabelP.of("object")).has("name", "marko").values("name").next());
    }

    @Test
    public void shouldNotThrowConcurrentModificationException() {
        this.graph.addVertex("name", "a");
        this.graph.addVertex("name", "b");
        this.graph.addVertex("name", "c");
        this.graph.addVertex("name", "d");
        this.graph.vertices().forEachRemaining(Vertex::remove);
        this.graph.tx().commit();
        assertEquals(0, IteratorUtils.count(this.graph.vertices()), 0);
    }

    @Test
    public void shouldTraverseWithoutLabels() {
        final AgensGraphAPI service = this.getGraph().getBaseGraph();

        final AgensTx tx = service.tx();
        final AgensNode n = service.createNode();
        tx.success();
        tx.close();

        final AgensTx tx2 = service.tx();
        assertEquals(0, IteratorUtils.count(n.labels().iterator()));
        assertEquals(1, IteratorUtils.count(graph.vertices()));
        graph.tx().close();
        tx2.close();
    }

    @Test
    public void shouldDoLabelSearch() {
        this.graph.addVertex(T.label, "Person", "name", "marko");
        this.graph.addVertex(T.label, "Person", "name", "john");
        Vertex pete = this.graph.addVertex(T.label, "Person", "name", "pete");
        this.graph.addVertex(T.label, "Monkey", "name", "pete");
        this.graph.tx().commit();
        assertEquals(3, this.g.V().has(T.label, "Person").count().next(), 0);
        pete.remove();
        this.graph.tx().commit();
        assertEquals(2, this.g.V().has(T.label, "Person").count().next(), 0);
    }

    @Test
    public void shouldNotGenerateVerticesOrEdgesForGraphVariables() {
        graph.tx().readWrite();
        graph.variables().set("namespace", "rdf-xml");
        tryCommit(graph, graph -> {
            assertEquals("rdf-xml", graph.variables().get("namespace").get());
            assertEquals(0, g.V().count().next().intValue());
            assertEquals(0, g.E().count().next().intValue());
            assertEquals(0, IteratorUtils.count(this.getBaseGraph().allNodes()));
            assertEquals(0, IteratorUtils.count(this.getBaseGraph().allRelationships()));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES, supported = false)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES, supported = false)
    public void shouldNotGenerateNodesAndRelationshipsForNoMultiPropertiesNoMetaProperties() {
        graph.tx().readWrite();
        tryCommit(graph, graph -> validateCounts(0, 0, 0, 0));
        Vertex vertex = graph.addVertex(T.label, "person");
        tryCommit(graph, graph -> validateCounts(1, 0, 1, 0));
        vertex.property("name", "marko");
        assertEquals("marko", vertex.value("name"));
        tryCommit(graph, graph -> validateCounts(1, 0, 1, 0));
        vertex.property("name", "okram");
        tryCommit(graph, g -> {
            validateCounts(1, 0, 1, 0);
            assertEquals("okram", vertex.value("name"));
        });
        VertexProperty vertexProperty = vertex.property("name");
        tryCommit(graph, graph -> {
            assertTrue(vertexProperty.isPresent());
            assertEquals("name", vertexProperty.key());
            assertEquals("okram", vertexProperty.value());
            validateCounts(1, 0, 1, 0);
        });
        try {
            vertexProperty.property("acl", "private");
        } catch (UnsupportedOperationException e) {
            assertEquals(VertexProperty.Exceptions.metaPropertiesNotSupported().getMessage(), e.getMessage());
        }
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    public void shouldGenerateNodesAndRelationshipsCorrectlyForVertexProperties() {

        graph.tx().readWrite();
        AgensVertex a = (AgensVertex) graph.addVertex("name", "marko", "name", "okram");
        AgensVertex b = (AgensVertex) graph.addVertex("name", "stephen", "location", "virginia");

        tryCommit(graph, graph -> {
            assertEquals(2, g.V().count().next().intValue());
            // assertEquals(2, a.properties("name").count().next().intValue());
            // assertEquals(1, b.properties("name").count().next().intValue());
            // assertEquals(1, b.properties("location").count().next().intValue());
            assertEquals(0, g.E().count().next().intValue());

            assertEquals(4l, this.getBaseGraph().execute("MATCH (n) RETURN COUNT(n)", null).next().get("COUNT(n)"));
            assertEquals(2l, this.getBaseGraph().execute("MATCH (n)-[r]->(m) RETURN COUNT(r)", null).next().get("COUNT(r)"));
            assertEquals(2l, this.getBaseGraph().execute("MATCH (a)-[r]->() WHERE id(a) = " + a.id() + " RETURN COUNT(r)", null).next().get("COUNT(r)"));
            final AtomicInteger counter = new AtomicInteger(0);
            a.getBaseVertex().relationships(AgensDirection.OUTGOING).forEach(relationship -> {
                assertEquals(Graph.Hidden.hide("name"), relationship.type());
                counter.incrementAndGet();
            });
            assertEquals(2, counter.getAndSet(0));
            this.getBaseGraph().execute("MATCH (a)-->(m) WHERE id(a) = " + a.id() + " RETURN labels(m)", null).forEachRemaining(results -> {
                assertEquals(true, ((List<String>) results.get("labels(m)")).contains(VertexProperty.DEFAULT_LABEL));
                counter.incrementAndGet();
            });
            assertEquals(2, counter.getAndSet(0));
            IteratorUtils.stream(a.getBaseVertex().relationships(AgensDirection.OUTGOING)).map(AgensRelationship::end).forEach(node -> {
                assertEquals(3, IteratorUtils.count(node.getKeys()));  // T.key, T.value, key
                assertEquals("name", node.getProperty(T.key.getAccessor()));
                assertTrue("marko".equals(node.getProperty(T.value.getAccessor())) || "okram".equals(node.getProperty(T.value.getAccessor())));
                assertEquals(0, node.degree(AgensDirection.OUTGOING, null));
                assertEquals(1, node.degree(AgensDirection.INCOMING, null));
                assertEquals(Graph.Hidden.hide("name"), node.relationships(AgensDirection.INCOMING).iterator().next().type());
                counter.incrementAndGet();
            });
            assertEquals(2, counter.getAndSet(0));

            assertEquals(2, IteratorUtils.count(b.getBaseVertex().getKeys()));
            assertEquals("stephen", b.getBaseVertex().getProperty("name"));
            assertEquals("virginia", b.getBaseVertex().getProperty("location"));
        });

        a.property("name", "the marko");
        tryCommit(graph, graph -> {
            assertEquals(2, g.V().count().next().intValue());
            //assertEquals(1, a.prope rties().count().next().intValue());
            //  assertEquals(1, b.properties("name").count().next().intValue());
            // assertEquals(1, b.properties("location").count().next().intValue());
            assertEquals(0, g.E().count().next().intValue());
            assertEquals(2l, this.getBaseGraph().execute("MATCH (n) RETURN COUNT(n)", null).next().get("COUNT(n)"));
            assertEquals(0l, this.getBaseGraph().execute("MATCH (n)-[r]->(m) RETURN COUNT(r)", null).next().get("COUNT(r)"));

            assertEquals(1, IteratorUtils.count(a.getBaseVertex().getKeys()));
            assertEquals("the marko", a.getBaseVertex().getProperty("name"));
            assertEquals(2, IteratorUtils.count(b.getBaseVertex().getKeys()));
            assertEquals("stephen", b.getBaseVertex().getProperty("name"));
            assertEquals("virginia", b.getBaseVertex().getProperty("location"));
        });

        a.property("name").remove();
        tryCommit(graph, graph -> {
            assertEquals(2, g.V().count().next().intValue());
            //    assertEquals(0, a.properties().count().next().intValue());
            //   assertEquals(2, b.properties().count().next().intValue());
            assertEquals(0, g.E().count().next().intValue());
            assertEquals(2l, this.getBaseGraph().execute("MATCH (n) RETURN COUNT(n)", null).next().get("COUNT(n)"));
            assertEquals(0l, this.getBaseGraph().execute("MATCH (n)-[r]->(m) RETURN COUNT(r)", null).next().get("COUNT(r)"));
            assertEquals(0, IteratorUtils.count(a.getBaseVertex().getKeys()));
            assertEquals(2, IteratorUtils.count(b.getBaseVertex().getKeys()));
        });

        graph.tx().commit();
        a.property("name", "the marko", "acl", "private");
        tryCommit(graph, graph -> {
            assertEquals(2, g.V().count().next().intValue());
            // assertEquals(1, a.properties("name").count().next().intValue());
            // assertEquals(1, b.properties("name").count().next().intValue());
            // assertEquals(1, b.properties("location").count().next().intValue());
            assertEquals(0, g.E().count().next().intValue());

            assertEquals(3l, this.getBaseGraph().execute("MATCH (n) RETURN COUNT(n)", null).next().get("COUNT(n)"));
            assertEquals(1l, this.getBaseGraph().execute("MATCH (n)-[r]->(m) RETURN COUNT(r)", null).next().get("COUNT(r)"));
            assertEquals(1l, this.getBaseGraph().execute("MATCH (a)-[r]->() WHERE id(a) = " + a.id() + " RETURN COUNT(r)", null).next().get("COUNT(r)"));
            final AtomicInteger counter = new AtomicInteger(0);
            a.getBaseVertex().relationships(AgensDirection.OUTGOING).forEach(relationship -> {
                assertEquals(Graph.Hidden.hide("name"), relationship.type());
                counter.incrementAndGet();
            });
            assertEquals(1, counter.getAndSet(0));
            this.getBaseGraph().execute("MATCH (a)-->(m) WHERE id(a) = " + a.id() + " RETURN labels(m)", null).forEachRemaining(results -> {
                assertEquals(true, ((List<String>) results.get("labels(m)")).contains(VertexProperty.DEFAULT_LABEL));
                counter.incrementAndGet();
            });
            assertEquals(1, counter.getAndSet(0));
            IteratorUtils.stream(a.getBaseVertex().relationships(AgensDirection.OUTGOING)).map(AgensRelationship::end).forEach(node -> {
                assertEquals(4, IteratorUtils.count(node.getKeys()));
                assertEquals("name", node.getProperty(T.key.getAccessor()));
                assertEquals("the marko", node.getProperty(T.value.getAccessor()));
                assertEquals("private", node.getProperty("acl"));
                assertEquals(0, node.degree(AgensDirection.OUTGOING, null));
                assertEquals(1, node.degree(AgensDirection.INCOMING, null));
                assertEquals(Graph.Hidden.hide("name"), node.relationships(AgensDirection.INCOMING).iterator().next().type());
                counter.incrementAndGet();
            });
            assertEquals(1, counter.getAndSet(0));

            assertEquals(1, IteratorUtils.count(a.getBaseVertex().getKeys()));
            assertTrue(a.getBaseVertex().hasProperty("name"));
            assertEquals(MultiMetaAgensTrait.VERTEX_PROPERTY_TOKEN, a.getBaseVertex().getProperty("name"));
            assertEquals(2, IteratorUtils.count(b.getBaseVertex().getKeys()));
            assertEquals("stephen", b.getBaseVertex().getProperty("name"));
            assertEquals("virginia", b.getBaseVertex().getProperty("location"));
        });

        a.property(VertexProperty.Cardinality.list, "name", "marko", "acl", "private");
        a.property(VertexProperty.Cardinality.list, "name", "okram", "acl", "public");
        a.property(VertexProperty.Cardinality.single, "name", "the marko", "acl", "private");
        tryCommit(graph, graph -> {
            assertEquals(2, g.V().count().next().intValue());
            //assertEquals(1, a.properties("name").count().next().intValue());
            //assertEquals(1, b.properties("name").count().next().intValue());
            //assertEquals(1, b.properties("location").count().next().intValue());
            assertEquals(0, g.E().count().next().intValue());

            assertEquals(3l, this.getBaseGraph().execute("MATCH (n) RETURN COUNT(n)", null).next().get("COUNT(n)"));
            assertEquals(1l, this.getBaseGraph().execute("MATCH (n)-[r]->(m) RETURN COUNT(r)", null).next().get("COUNT(r)"));
            assertEquals(1l, this.getBaseGraph().execute("MATCH (a)-[r]->() WHERE id(a) = " + a.id() + " RETURN COUNT(r)", null).next().get("COUNT(r)"));
            final AtomicInteger counter = new AtomicInteger(0);
            a.getBaseVertex().relationships(AgensDirection.OUTGOING).forEach(relationship -> {
                assertEquals(Graph.Hidden.hide("name"), relationship.type());
                counter.incrementAndGet();
            });
            assertEquals(1, counter.getAndSet(0));
            this.getBaseGraph().execute("MATCH (a)-->(m) WHERE id(a) = " + a.id() + " RETURN labels(m)", null).forEachRemaining(results -> {
                assertEquals(true, ((List<String>) results.get("labels(m)")).contains(VertexProperty.DEFAULT_LABEL));
                counter.incrementAndGet();
            });
            assertEquals(1, counter.getAndSet(0));
            IteratorUtils.stream(a.getBaseVertex().relationships(AgensDirection.OUTGOING)).map(AgensRelationship::end).forEach(node -> {
                assertEquals(4, IteratorUtils.count(node.getKeys()));
                assertEquals("name", node.getProperty(T.key.getAccessor()));
                assertEquals("the marko", node.getProperty(T.value.getAccessor()));
                assertEquals("private", node.getProperty("acl"));
                assertEquals(0, node.degree(AgensDirection.OUTGOING, null));
                assertEquals(1, node.degree(AgensDirection.INCOMING, null));
                assertEquals(Graph.Hidden.hide("name"), node.relationships(AgensDirection.INCOMING).iterator().next().type());
                counter.incrementAndGet();
            });
            assertEquals(1, counter.getAndSet(0));

            assertEquals(1, IteratorUtils.count(a.getBaseVertex().getKeys()));
            assertTrue(a.getBaseVertex().hasProperty("name"));
            assertEquals(MultiMetaAgensTrait.VERTEX_PROPERTY_TOKEN, a.getBaseVertex().getProperty("name"));
            assertEquals(2, IteratorUtils.count(b.getBaseVertex().getKeys()));
            assertEquals("stephen", b.getBaseVertex().getProperty("name"));
            assertEquals("virginia", b.getBaseVertex().getProperty("location"));
        });
    }

    @Test
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_MULTI_PROPERTIES)
    @FeatureRequirement(featureClass = Graph.Features.VertexFeatures.class, feature = Graph.Features.VertexFeatures.FEATURE_META_PROPERTIES)
    public void shouldNotGenerateNodesAndRelationshipsForMultiPropertiesWithSingle() {
        graph.tx().readWrite();
        tryCommit(graph, graph -> validateCounts(0, 0, 0, 0));
        Vertex vertex = graph.addVertex(T.label, "person");
        tryCommit(graph, graph -> validateCounts(1, 0, 1, 0));
        vertex.property(VertexProperty.Cardinality.list, "name", "marko");
        assertEquals("marko", vertex.value("name"));
        tryCommit(graph, graph -> validateCounts(1, 0, 1, 0));
        vertex.property(VertexProperty.Cardinality.single, "name", "okram");
        tryCommit(graph, graph -> {
            validateCounts(1, 0, 1, 0);
            assertEquals("okram", vertex.value("name"));
        });
        VertexProperty vertexProperty = vertex.property("name");
        tryCommit(graph, graph -> {
            assertTrue(vertexProperty.isPresent());
            assertEquals("name", vertexProperty.key());
            assertEquals("okram", vertexProperty.value());
            validateCounts(1, 0, 1, 0);
        });

        // now make it a meta property (and thus, force node/relationship creation)
        vertexProperty.property("acl", "private");
        tryCommit(graph, graph -> {
            assertEquals("private", vertexProperty.value("acl"));
            validateCounts(1, 0, 2, 1);
        });

    }

    @Test
    public void shouldSupportAgensMultiLabels() {
        final AgensVertex vertex = (AgensVertex) graph.addVertex(T.label, "animal::person", "name", "marko");
        tryCommit(graph, graph -> {
            assertTrue(vertex.label().equals("animal::person"));
            assertEquals(2, vertex.labels().size());
            assertTrue(vertex.labels().contains("person"));
            assertTrue(vertex.labels().contains("animal"));
            assertEquals(2, IteratorUtils.count(vertex.getBaseVertex().labels().iterator()));
        });

        vertex.addLabel("organism");
        tryCommit(graph, graph -> {
            assertTrue(vertex.label().equals("animal::organism::person"));
            assertEquals(3, vertex.labels().size());
            assertTrue(vertex.labels().contains("person"));
            assertTrue(vertex.labels().contains("animal"));
            assertTrue(vertex.labels().contains("organism"));
            assertEquals(3, IteratorUtils.count(vertex.getBaseVertex().labels().iterator()));
        });

        vertex.removeLabel("person");
        tryCommit(graph, graph -> {
            assertTrue(vertex.label().equals("animal::organism"));
            assertEquals(2, vertex.labels().size());
            assertTrue(vertex.labels().contains("animal"));
            assertTrue(vertex.labels().contains("organism"));
        });

        vertex.addLabel("organism"); // repeat add
        vertex.removeLabel("person"); // repeat remove
        tryCommit(graph, graph -> {
            assertTrue(vertex.label().equals("animal::organism"));
            assertEquals(2, vertex.labels().size());
            assertTrue(vertex.labels().contains("animal"));
            assertTrue(vertex.labels().contains("organism"));
            assertEquals(2, IteratorUtils.count(vertex.getBaseVertex().labels().iterator()));
        });

        assertEquals(Long.valueOf(0), g.V().has(T.label, "organism").count().next());
        assertEquals(Long.valueOf(1), g.V().has(T.label, LabelP.of("organism")).count().next());
        assertEquals(Long.valueOf(1), g.V().has(T.label, LabelP.of("animal")).count().next());

        vertex.removeLabel("organism");
        vertex.removeLabel("animal");
        assertEquals(0, vertex.labels().size());
        vertex.addLabel("organism-animal");
        tryCommit(graph, graph -> {
            assertEquals(Long.valueOf(0), g.V().has(T.label, LabelP.of("organism")).count().next());
            assertEquals(Long.valueOf(0), g.V().has(T.label, LabelP.of("animal")).count().next());
            assertEquals(Long.valueOf(0), g.V().map(Traverser::get).has(T.label, LabelP.of("organism")).count().next());
            assertEquals(Long.valueOf(0), g.V().map(Traverser::get).has(T.label, LabelP.of("animal")).count().next());
            //
            assertEquals(Long.valueOf(1), g.V().has(T.label, LabelP.of("organism-animal")).count().next());
            assertEquals(Long.valueOf(1), g.V().has(T.label, "organism-animal").count().next());
            assertEquals(Long.valueOf(1), g.V().map(Traverser::get).has(T.label, LabelP.of("organism-animal")).count().next());
            assertEquals(Long.valueOf(1), g.V().map(Traverser::get).has(T.label, "organism-animal").count().next());
        });
    }

}
