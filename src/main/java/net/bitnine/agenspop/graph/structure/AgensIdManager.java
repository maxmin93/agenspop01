package net.bitnine.agenspop.graph.structure;

import java.util.UUID;
import java.util.stream.Stream;

public enum AgensIdManager implements AgensGraph.IdManager {
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
        public java.util.UUID getNextId(final AgensGraph graph) {
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
