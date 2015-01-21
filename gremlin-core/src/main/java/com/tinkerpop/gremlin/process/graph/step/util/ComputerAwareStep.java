package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.EngineDependent;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ComputerAwareStep<S, E> extends AbstractStep<S, E> implements EngineDependent {

    protected boolean onGraphComputer;
    private Iterator<Traverser<E>> previousIterator = Collections.emptyIterator();

    public ComputerAwareStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    protected Traverser<E> processNextStart() throws NoSuchElementException {
        while (true) {
            if (this.previousIterator.hasNext())
                return this.previousIterator.next();
            this.previousIterator = this.onGraphComputer ? this.computerAlgorithm() : this.standardAlgorithm();
        }
    }

    @Override
    public void onEngine(final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER)) {
            this.onGraphComputer = true;
            this.traverserStepIdSetByChild = true;
        }
    }

    @Override
    public ComputerAwareStep<S, E> clone() throws CloneNotSupportedException {
        final ComputerAwareStep<S, E> clone = (ComputerAwareStep<S, E>) super.clone();
        clone.previousIterator = Collections.emptyIterator();
        return clone;
    }

    protected abstract Iterator<Traverser<E>> standardAlgorithm() throws NoSuchElementException;

    protected abstract Iterator<Traverser<E>> computerAlgorithm() throws NoSuchElementException;

    //////

    public class EndStep extends AbstractStep<S, S> {

        public EndStep(final Traversal traversal) {
            super(traversal);
            this.traverserStepIdSetByChild = true;
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            final Traverser.Admin<S> start = this.starts.next();
            start.setStepId(ComputerAwareStep.this.getNextStep().getId());
            return start;
        }

        @Override
        public String toString() {
            return TraversalHelper.makeStepString(this);
        }
    }

}