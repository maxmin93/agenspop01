package net.bitnine.agenspop.graph.process.traversal.strategy.optimization;

import net.bitnine.agenspop.graph.process.traversal.step.sideEffect.AgensGraphStep;
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

/*
AgensGraphStepStrategy::traversal1 = [GraphStep(vertex,[]), HasStep([~label.eq(person), name.eq(marko)])]

AgensGraphStepStrategy::AgensGraphStep1 = AgensGraphStep(vertex,[])
AgensGraphStepStrategy::hasContainer = ~label.eq(person)
AgensGraphStepStrategy::hasContainer = name.eq(marko)
AgensGraphStepStrategy::AgensGraphStep2 = AgensGraphStep(vertex,[~label.eq(person), name.eq(marko)])

AgensGraphStepStrategy::traversal2 = [AgensGraphStep(vertex,[~label.eq(person), name.eq(marko)])]
 */
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        System.out.println("AgensGraphStepStrategy::traversal1 = "+traversal.toString());

        // 전체 traversal 에서 graphStep 들 반복
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            // graphStep 을 AgensGraphStep 으로 변경 (바꿔치기)
            final AgensGraphStep<?, ?> agensGraphStep = new AgensGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, agensGraphStep, traversal);
            System.out.println("AgensGraphStepStrategy::AgensGraphStep1 = "+agensGraphStep.toString());

            Step<?, ?> currentStep = agensGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                // GraphStep 으로부터 이어지는 HasStep 인 경우
                if (currentStep instanceof HasStep) {
                    // HasStep 의 모든 filter container 들에 대해서
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        // 지정 id 가 있거나, eq 또는 within 연산자가 있는 경우가 아니면
                        //   agensGraphStep 에 hasContainer 를 추가(??)
                        System.out.println("AgensGraphStepStrategy::hasContainer = "+hasContainer.toString());
                        if (!GraphStep.processHasContainerIds(agensGraphStep, hasContainer))
                            agensGraphStep.addHasContainer(hasContainer);
                    }
                    // 현재 단계를 이전 단계로 복사하고, 현재 단계를 전체 traversal 에서 제거 (축약?)
                    // void copyLabels(final Step<?, ?> fromStep, final Step<?, ?> toStep, final boolean moveLabels)
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }

            System.out.println("AgensGraphStepStrategy::AgensGraphStep2 = "+agensGraphStep.toString());
        }
        System.out.println("AgensGraphStepStrategy::traversal2 = "+traversal.toString());
    }

    public static AgensGraphStepStrategy instance() {
        return INSTANCE;
    }
}

/*
 ** 참고 https://kelvinlawrence.net/book/Gremlin-Graph-Guide.html
 ** 4.19.2. Analyzing where time is spent - introducing profile

g.V().has('region','US-TX').out().has('region','US-CA').
                            out().has('country','DE').profile()
==>
Step                                       Count  Traversers  Time (ms)    % Dur
===============================================================================
TinkerGraphStep(vertex,[region.eq(US-TX)])   26          26      1.810     9.71
VertexStep(OUT,vertex)                      701         701      0.877     4.70
HasStep([region.eq(US-CA)])                  47          47      0.561     3.01
VertexStep(OUT,vertex)                     3464        3464     12.035    64.54
NoOpBarrierStep(2500)                      3464         224      3.157    16.93
HasStep([country.eq(DE)])                    59           4      0.206     1.11
   >TOTAL

 */