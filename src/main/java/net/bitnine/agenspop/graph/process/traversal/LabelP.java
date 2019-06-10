package net.bitnine.agenspop.graph.process.traversal;

import net.bitnine.agenspop.graph.structure.AgensVertex;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.io.Serializable;
import java.util.function.BiPredicate;

public final class LabelP extends P<String> {

    private LabelP(final String label) {
        super(LabelBiPredicate.instance(), label);
    }

    public static P<String> of(final String label) {
        return new LabelP(label);
    }

    public static final class LabelBiPredicate implements BiPredicate<String, String>, Serializable {

        private static final LabelBiPredicate INSTANCE = new LabelBiPredicate();

        private LabelBiPredicate() {
        }

        @Override
        public boolean test(final String labels, final String checkLabel) {
            return labels.equals(checkLabel)
                    || labels.contains(AgensVertex.LABEL_DELIMINATOR + checkLabel)
                    || labels.contains(checkLabel + AgensVertex.LABEL_DELIMINATOR);
        }

        public static LabelBiPredicate instance() {
            return INSTANCE;
        }
    }

}