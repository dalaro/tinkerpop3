package com.tinkerpop.gremlin.hmr.structure;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.structure.Element;

public class HMRGraphStep<E extends Element> extends GraphStep<E> {

    private final HMRGraph graph;

    public HMRGraphStep(Traversal traversal, Class<E> returnClass, HMRGraph graph) {
        super(traversal, returnClass);
        this.graph = graph;
    }

    /* TODO override generateTraverserIterator */
}
