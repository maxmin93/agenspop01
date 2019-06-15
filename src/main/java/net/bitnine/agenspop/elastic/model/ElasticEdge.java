package net.bitnine.agenspop.elastic.model;

public interface ElasticEdge extends ElasticElement {

    public static final String DEFAULT_LABEL = "edge";

    Integer getSid();
    Integer getTid();

    // String type();       // ??

//    ElasticVertex start();
//    ElasticVertex end();
//    ElasticVertex other(ElasticVertex node);

/*
    public Iterator<ElasticVertexWrapper> vertices(final Direction direction);

    public default ElasticVertexWrapper outVertex() {
        return this.vertices(Direction.OUT).next();
    }
    public default ElasticVertexWrapper inVertex() {
        return this.vertices(Direction.IN).next();
    }
    public default Iterator<ElasticVertexWrapper> bothVertices() {
        return this.vertices(Direction.BOTH);
    }

    @Override
    public Iterator<HashMap.Entry<String,Object>> properties(final String... propertyKeys);
*/

    /**
     * Common exceptions to use with an edge.
     */
    public static class Exceptions extends ElasticElement.Exceptions {

        private Exceptions() {
        }

        public static UnsupportedOperationException userSuppliedIdsNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers");
        }

        public static UnsupportedOperationException userSuppliedIdsOfThisTypeNotSupported() {
            return new UnsupportedOperationException("Edge does not support user supplied identifiers of this type");
        }

        public static IllegalStateException edgeRemovalNotSupported() {
            return new IllegalStateException("Edge removal are not supported");
        }
    }
}
