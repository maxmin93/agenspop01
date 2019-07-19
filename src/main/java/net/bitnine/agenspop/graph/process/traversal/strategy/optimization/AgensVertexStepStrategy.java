package net.bitnine.agenspop.graph.process.traversal.strategy.optimization;

import net.bitnine.agenspop.graph.process.traversal.step.sideEffect.AgensVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public class AgensVertexStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AgensVertexStepStrategy INSTANCE = new AgensVertexStepStrategy();

    private AgensVertexStepStrategy() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final VertexStep originalVertexStep : TraversalHelper.getStepsOfClass(VertexStep.class, traversal)) {
            final AgensVertexStep<?> agensVertexStep = new AgensVertexStep<>(originalVertexStep);
            TraversalHelper.replaceStep(originalVertexStep, agensVertexStep, traversal);
            Step<?, ?> currentStep = agensVertexStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        agensVertexStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static AgensVertexStepStrategy instance() {
        return INSTANCE;
    }

}
