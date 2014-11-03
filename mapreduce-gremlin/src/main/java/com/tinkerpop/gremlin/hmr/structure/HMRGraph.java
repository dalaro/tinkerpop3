package com.tinkerpop.gremlin.hmr.structure;

import com.tinkerpop.gremlin.hmr.process.computer.HMRComputer;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Transaction;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Optional;

public class HMRGraph implements Graph {

    private final HMRVariables variables;

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V() {
        final GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(this);
        return traversal.addStep(new HMRGraphStep<>(traversal, Vertex.class, this));
    }

    @Override
    public GraphTraversal<Edge, Edge> E() {
        final GraphTraversal<Edge, Edge> traversal = new DefaultGraphTraversal<>(this);
        return traversal.addStep(new HMRGraphStep<>(traversal, Edge.class, this));
    }

    @Override
    public GraphComputer compute(Class... graphComputerClass) {
        HMRComputerHelper.validateComputeArguments(graphComputerClass);
        if (graphComputerClass.length == 0 || graphComputerClass[0].equals(HMRComputer.class))
            return new HMRComputer(this);
        else
            throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass[0]);
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public HMRVariables variables() {
        return this.variables;
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException("Not yet implemented"); // TODO implement
    }

    private HMRGraph(final Configuration configuration) {
        this.variables = new HMRVariables(new HMRConfiguration(configuration));
    }

    public static HMRGraph open() {
        return HMRGraph.open(null);
    }

    public static HMRGraph open(final Configuration configuration) {
        return new HMRGraph(Optional.ofNullable(configuration).orElse(new BaseConfiguration()));
    }
}
