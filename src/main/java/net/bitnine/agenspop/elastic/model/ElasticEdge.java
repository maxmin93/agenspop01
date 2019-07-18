package net.bitnine.agenspop.elastic.model;

public interface ElasticEdge extends ElasticElement {

    public static final String DEFAULT_LABEL = "edge";

    String getSid();
    String getTid();

/*
    // **NOTE: vertex index 에서 탐색
    //
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
