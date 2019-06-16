package net.bitnine.agenspop.graph.structure;

import net.bitnine.agenspop.elastic.ElasticGraphAPI;
import net.bitnine.agenspop.elastic.ElasticTx;
import net.bitnine.agenspop.graph.process.traversal.strategy.optimization.AgensGraphCountStrategy;
import net.bitnine.agenspop.graph.process.traversal.strategy.optimization.AgensGraphStepStrategy;

import net.bitnine.agenspop.graph.structure.trait.AgensTrait;
import net.bitnine.agenspop.graph.structure.trait.SimpleAgensTrait;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphson;

/**
 * An in-memory (with optional persistence on calls to {@link #close()}), reference implementation of the property
 * graph interfaces provided by TinkerPop.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
public final class AgensGraph implements Graph, WrappedGraph<ElasticGraphAPI> {

    public static final Logger LOGGER = LoggerFactory.getLogger(AgensGraph.class);
    static {
        TraversalStrategies.GlobalCache.registerStrategies(AgensGraph.class
                , TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
                        AgensGraphStepStrategy.instance(),
                        AgensGraphCountStrategy.instance()
                ));
    }

    private static final Configuration EMPTY_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, AgensGraph.class.getName());
    }};

    public static final String GREMLIN_AGENSGRAPH_VERTEX_ID_MANAGER = "gremlin.agensgraph.vertexIdManager";
    public static final String GREMLIN_AGENSGRAPH_EDGE_ID_MANAGER = "gremlin.agensgraph.edgeIdManager";
    public static final String GREMLIN_AGENSGRAPH_VERTEX_PROPERTY_ID_MANAGER = "gremlin.agensgraph.vertexPropertyIdManager";
    public static final String GREMLIN_AGENSGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY = "gremlin.agensgraph.defaultVertexPropertyCardinality";
    public static final String GREMLIN_AGENSGRAPH_GRAPH_LOCATION = "gremlin.agensgraph.graphLocation";
    public static final String GREMLIN_AGENSGRAPH_GRAPH_FORMAT = "gremlin.agensgraph.graphFormat";
    public static final String GREMLIN_AGENSGRAPH_GRAPH_NAME = "gremlin.agensgraph.graphName";      // added

    private final AgensGraphFeatures features = new AgensGraphFeatures();

    protected ElasticGraphAPI baseGraph;
    protected BaseConfiguration configuration = new BaseConfiguration();
    protected AgensTrait trait;

    private final AgensTransaction transaction = new AgensTransaction();
    protected AgensGraphVariables graphVariables;

    protected AtomicLong currentId = new AtomicLong(-1L);
    protected Map<Object, Vertex> vertices = new ConcurrentHashMap<>();
    protected Map<Object, Edge> edges = new ConcurrentHashMap<>();

    protected Object graphComputerView = null;                  // excluded
    protected AgensIndex<AgensVertex> vertexIndex = null;
    protected AgensIndex<AgensEdge> edgeIndex = null;

    protected IdManager<?> vertexIdManager;
    protected IdManager<?> edgeIdManager;
    protected IdManager<?> vertexPropertyIdManager;
    protected VertexProperty.Cardinality defaultVertexPropertyCardinality;

    protected String graphName;
    private String graphLocation;
    private String graphFormat;

    private void initialize(final ElasticGraphAPI baseGraph, final Configuration configuration) {
        this.configuration.copy(configuration);
        this.baseGraph = baseGraph;
        this.graphVariables = new AgensGraphVariables(this);
        this.trait = SimpleAgensTrait.instance();

        this.tx().readWrite();

        vertexIdManager = selectIdManager(configuration, GREMLIN_AGENSGRAPH_VERTEX_ID_MANAGER, Vertex.class);
        edgeIdManager = selectIdManager(configuration, GREMLIN_AGENSGRAPH_EDGE_ID_MANAGER, Edge.class);
        vertexPropertyIdManager = selectIdManager(configuration, GREMLIN_AGENSGRAPH_VERTEX_PROPERTY_ID_MANAGER, VertexProperty.class);
        defaultVertexPropertyCardinality = VertexProperty.Cardinality.valueOf(
                configuration.getString(
                        GREMLIN_AGENSGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY
                        , VertexProperty.Cardinality.single.name()));

        // added
        graphName = configuration.getString(GREMLIN_AGENSGRAPH_GRAPH_NAME, "default");
        graphLocation = configuration.getString(GREMLIN_AGENSGRAPH_GRAPH_LOCATION, null);
        graphFormat = configuration.getString(GREMLIN_AGENSGRAPH_GRAPH_FORMAT, null);

        if ((graphLocation != null && null == graphFormat) || (null == graphLocation && graphFormat != null)) {
            throw new IllegalStateException(String.format("The %s and %s must both be specified if either is present",
                    GREMLIN_AGENSGRAPH_GRAPH_LOCATION, GREMLIN_AGENSGRAPH_GRAPH_FORMAT));
        }
        if (graphLocation != null) loadGraph();

        this.tx().commit();
    }

    protected AgensGraph(final ElasticGraphAPI baseGraph, final Configuration configuration) {
        this.initialize(baseGraph, configuration);
    }

    public static AgensGraph open(final ElasticGraphAPI baseGraph){
        final Configuration config = new BaseConfiguration();
        config.setProperty( GREMLIN_AGENSGRAPH_GRAPH_NAME, AgensFactory.defaultGraphName());
        return new AgensGraph(baseGraph, config);
    }

    public static AgensGraph open(final ElasticGraphAPI baseGraph, final Configuration config){
        if( !config.containsKey( GREMLIN_AGENSGRAPH_GRAPH_NAME )){
            config.setProperty( GREMLIN_AGENSGRAPH_GRAPH_NAME, AgensFactory.defaultGraphName());
        }
        return new AgensGraph(baseGraph, config);
    }

    ///////////////////////////////////////////////////////

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Variables variables() {
        if (null == this.graphVariables)
            this.graphVariables = new AgensGraphVariables(this);
        return this.graphVariables;
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).onMapper(mapper -> mapper.addRegistry(AgensIoRegistryV1.instance())).create();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, graphName + "][vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public String name() {
        return this.graphName;
    }

    public void clear() {
        this.vertices.clear();
        this.edges.clear();
        this.graphVariables = null;
        this.currentId.set(-1L);
        this.vertexIndex = null;
        this.edgeIndex = null;
        this.graphComputerView = null;
    }

    /**
     * This method only has an effect if the {@link #GREMLIN_AGENSGRAPH_GRAPH_LOCATION} is set, in which case the
     * data in the graph is persisted to that location. This method may be called multiple times and does not release
     * resources.
     */
    @Override
    public void close() {
        if (graphLocation != null) saveGraph();
    }

    @Override
    public Transaction tx() {
//        throw Exceptions.transactionsNotSupported();
        return this.transaction;
    }

    @Override
    public ElasticGraphAPI getBaseGraph() {
        return this.baseGraph;
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Features features() {
        return features;
    }

    ///////////////////////////////////////////////////////

    private void loadGraph() {
        final File f = new File(graphLocation);
        if (f.exists() && f.isFile()) {
            try {
                if (graphFormat.equals("graphml")) {
                    io(graphml()).readGraph(graphLocation);
                } else if (graphFormat.equals("graphson")) {
                    io(graphson()).readGraph(graphLocation);
                } else if (graphFormat.equals("gryo")) {
                    io(IoCore.gryo()).readGraph(graphLocation);
                } else {
                    io(IoCore.createIoBuilder(graphFormat)).readGraph(graphLocation);
                }
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Could not load graph at %s with %s", graphLocation, graphFormat), ex);
            }
        }
    }

    private void saveGraph() {
        final File f = new File(graphLocation);
        if (f.exists()) {
            f.delete();
        } else {
            final File parent = f.getParentFile();

            // the parent would be null in the case of an relative path if the graphLocation was simply: "f.gryo"
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
        }

        try {
            if (graphFormat.equals("graphml")) {
                io(graphml()).writeGraph(graphLocation);
            } else if (graphFormat.equals("graphson")) {
                io(graphson()).writeGraph(graphLocation);
            } else if (graphFormat.equals("gryo")) {
                io(IoCore.gryo()).writeGraph(graphLocation);
            } else {
                io(IoCore.createIoBuilder(graphFormat)).writeGraph(graphLocation);
            }
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Could not save graph at %s with %s", graphLocation, graphFormat), ex);
        }
    }

    ///////////////////////////////////////////////////////

    ////////////// STRUCTURE API METHODS //////////////////

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = vertexIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = vertexIdManager.getNextId(this);
        }

        // @Todo : type of idValue must be Long!!
        final Vertex vertex = new AgensVertex(
                this.baseGraph.createVertex(new Integer(idValue.toString()), label, graphName), this);
        this.vertices.put(vertex.id(), vertex);

        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.list, keyValues);
        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return createElementIterator(Vertex.class, vertices, vertexIdManager, vertexIds);
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return createElementIterator(Edge.class, edges, edgeIdManager, edgeIds);
    }

    private <T extends Element> Iterator<T> createElementIterator(final Class<T> clazz, final Map<Object, T> elements,
                                                                  final IdManager idManager,
                                                                  final Object... ids) {
        final Iterator<T> iterator;
        if (0 == ids.length) {
            iterator = elements.values().iterator();
        } else {
            final List<Object> idList = Arrays.asList(ids);
            validateHomogenousIds(idList);

            // if the type is of Element - have to look each up because it might be an Attachable instance or
            // other implementation. the assumption is that id conversion is not required for detached
            // stuff - doesn't seem likely someone would detach a Titan vertex then try to expect that
            // vertex to be findable in OrientDB
            return clazz.isAssignableFrom(ids[0].getClass()) ?
                    IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(clazz.cast(id).id())).iterator(), Objects::nonNull)
                    : IteratorUtils.filter(IteratorUtils.map(idList, id -> elements.get(idManager.convert(id))).iterator(), Objects::nonNull);
        }
        return iterator;
    }

    private void validateHomogenousIds(final List<Object> ids) {
        final Iterator<Object> iterator = ids.iterator();
        Object id = iterator.next();
        if (id == null)
            throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        final Class firstClass = id.getClass();
        while (iterator.hasNext()) {
            id = iterator.next();
            if (id == null || !id.getClass().equals(firstClass))
                throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
        }
    }

    ///////////////////////////////////////////////////////

    public class AgensGraphFeatures implements Features {

        private final AgensGraphGraphFeatures graphFeatures = new AgensGraphGraphFeatures();
        private final AgensGraphEdgeFeatures edgeFeatures = new AgensGraphEdgeFeatures();
        private final AgensGraphVertexFeatures vertexFeatures = new AgensGraphVertexFeatures();

        private AgensGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }

    }

    public class AgensGraphVertexFeatures implements Features.VertexFeatures {

        private final AgensGraphVertexPropertyFeatures vertexPropertyFeatures = new AgensGraphVertexPropertyFeatures();

        private AgensGraphVertexFeatures() {
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return defaultVertexPropertyCardinality;
        }
    }

    public class AgensGraphEdgeFeatures implements Features.EdgeFeatures {

        private AgensGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return edgeIdManager.allow(id);
        }
    }

    public class AgensGraphGraphFeatures implements Features.GraphFeatures {

        private AgensGraphGraphFeatures() {
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

    }

    public class AgensGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private AgensGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return vertexIdManager.allow(id);
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link Vertex} or {@link Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null == this.vertexIndex) this.vertexIndex = new AgensIndex<>(this, AgensVertex.class);
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null == this.edgeIndex) this.edgeIndex = new AgensIndex<>(this, AgensEdge.class);
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link Vertex} or {@link Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            if (null != this.vertexIndex) this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            if (null != this.edgeIndex) this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Return all the keys currently being index for said element class  ({@link Vertex} or {@link Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return null == this.vertexIndex ? Collections.emptySet() : this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return null == this.edgeIndex ? Collections.emptySet() : this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Construct an {@link AgensGraph.IdManager} from the AgensGraph {@code Configuration}.
     */
    private static IdManager<?> selectIdManager(final Configuration config, final String configKey, final Class<? extends Element> clazz) {
        final String vertexIdManagerConfigValue = config.getString(configKey, DefaultIdManager.ANY.name());
        try {
            return DefaultIdManager.valueOf(vertexIdManagerConfigValue);
        } catch (IllegalArgumentException iae) {
            try {
                return (IdManager) Class.forName(vertexIdManagerConfigValue).newInstance();
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Could not configure AgensGraph %s id manager with %s", clazz.getSimpleName(), vertexIdManagerConfigValue));
            }
        }
    }

    /**
     * AgensGraph will use an implementation of this interface to generate identifiers when a user does not supply
     * them and to handle identifier conversions when querying to provide better flexibility with respect to
     * handling different data types that mean the same thing.  For example, the
     * {@link DefaultIdManager#LONG} implementation will allow {@code g.vertices(1l, 2l)} and
     * {@code g.vertices(1, 2)} to both return values.
     *
     * @param <T> the id type
     */
    public interface IdManager<T> {
        /**
         * Generate an identifier which should be unique to the {@link AgensGraph} instance.
         */
        T getNextId(final AgensGraph graph);

        /**
         * Convert an identifier to the type required by the manager.
         */
        T convert(final Object id);

        /**
         * Determine if an identifier is allowed by this manager given its type.
         */
        boolean allow(final Object id);
    }

    /**
     * A default set of {@link IdManager} implementations for common identifier types.
     */
    public enum DefaultIdManager implements IdManager {
        /**
         * Manages identifiers of type {@code Long}. Will convert any class that extends from {@link Number} to a
         * {@link Long} and will also attempt to convert {@code String} values
         */
        LONG {
            @Override
            public Long getNextId(final AgensGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Long)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).longValue();
                else if (id instanceof String)
                    return Long.parseLong((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@code Integer}. Will convert any class that extends from {@link Number} to a
         * {@link Integer} and will also attempt to convert {@code String} values
         */
        INTEGER {
            @Override
            public Integer getNextId(final AgensGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).map(Long::intValue).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof Integer)
                    return id;
                else if (id instanceof Number)
                    return ((Number) id).intValue();
                else if (id instanceof String)
                    return Integer.parseInt((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to Integer but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof Number || id instanceof String;
            }
        },

        /**
         * Manages identifiers of type {@link java.util.UUID}. Will convert {@code String} values to
         * {@link java.util.UUID}.
         */
        UUID {
            @Override
            public UUID getNextId(final AgensGraph graph) {
                return java.util.UUID.randomUUID();
            }

            @Override
            public Object convert(final Object id) {
                if (null == id)
                    return null;
                else if (id instanceof java.util.UUID)
                    return id;
                else if (id instanceof String)
                    return java.util.UUID.fromString((String) id);
                else
                    throw new IllegalArgumentException(String.format("Expected an id that is convertible to UUID but received %s", id.getClass()));
            }

            @Override
            public boolean allow(final Object id) {
                return id instanceof UUID || id instanceof String;
            }
        },

        /**
         * Manages identifiers of any type.  This represents the default way {@link AgensGraph} has always worked.
         * In other words, there is no identifier conversion so if the identifier of a vertex is a {@code Long}, then
         * trying to request it with an {@code Integer} will have no effect. Also, like the original
         * {@link AgensGraph}, it will generate {@link Long} values for identifiers.
         */
        ANY {
            @Override
            public Long getNextId(final AgensGraph graph) {
                return Stream.generate(() -> (graph.currentId.incrementAndGet())).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
            }

            @Override
            public Object convert(final Object id) {
                return id;
            }

            @Override
            public boolean allow(final Object id) {
                return true;
            }
        }
    }

    ///////////////////////////////////////////////

    class AgensTransaction extends AbstractThreadLocalTransaction {

        protected final ThreadLocal<ElasticTx> threadLocalTx = ThreadLocal.withInitial(() -> null);

        public AgensTransaction() {
            super(AgensGraph.this);
        }

        @Override
        public void doOpen() {
            threadLocalTx.set(getBaseGraph().tx());
        }

        @Override
        public void doCommit() throws TransactionException {
            try (ElasticTx tx = threadLocalTx.get()) {
                tx.success();
            } catch (Exception ex) {
                throw new TransactionException(ex);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public void doRollback() throws TransactionException {
            try (ElasticTx tx = threadLocalTx.get()) {
                tx.failure();
            } catch (Exception e) {
                throw new TransactionException(e);
            } finally {
                threadLocalTx.remove();
            }
        }

        @Override
        public boolean isOpen() {
            return (threadLocalTx.get() != null);
        }
    }
}