package com.tinkerpop.gremlin.process.traversers;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleTraverserGenerator implements TraverserGenerator {

    public <S> Traverser.Admin<S> generate(final S start, final Step<S,?> startStep, final long initialBulk) {
        final SimpleTraverser<S> traverser = new SimpleTraverser<>(start, startStep.getTraversal().sideEffects());
        traverser.setBulk(initialBulk);
        return traverser;
    }
}
