package net.bitnine.agens.agenspop.graph.process.traversal.strategy.optimization;

import net.bitnine.agens.agenspop.graph.process.traversal.step.sideEffect.AgensGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public final class AgensGraphStepStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final AgensGraphStepStrategy INSTANCE = new AgensGraphStepStrategy();

    private AgensGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final AgensGraphStep<?, ?> tinkerGraphStep = new AgensGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, tinkerGraphStep, traversal);
            Step<?, ?> currentStep = tinkerGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(tinkerGraphStep, hasContainer))
                            tinkerGraphStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static AgensGraphStepStrategy instance() {
        return INSTANCE;
    }
}