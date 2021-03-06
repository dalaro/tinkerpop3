package com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GroupByStep;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.commons.configuration.Configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupByMapReduce implements MapReduce<Object, Collection, Object, Object, Map> {

    public static final String GROUP_BY_STEP_SIDE_EFFECT_KEY = "gremlin.groupByStep.sideEffectKey";
    public static final String GROUP_BY_STEP_STEP_LABEL = "gremlin.groupByStep.stepLabel";

    private String sideEffectKey;
    private Traversal<?, ?> traversal;
    private String groupByStepKey;
    private Function reduceFunction;
    private Supplier<Map> mapSupplier;

    private GroupByMapReduce() {

    }

    public GroupByMapReduce(final GroupByStep step) {
        this.groupByStepKey = step.getLabel();
        this.sideEffectKey = step.getSideEffectKey();
        this.reduceFunction = step.getReduceFunction();
        this.traversal = step.getTraversal();
        this.mapSupplier = this.traversal.sideEffects().<Map>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
        configuration.setProperty(GROUP_BY_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
        configuration.setProperty(GROUP_BY_STEP_STEP_LABEL, this.groupByStepKey);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(GROUP_BY_STEP_SIDE_EFFECT_KEY);
        this.groupByStepKey = configuration.getString(GROUP_BY_STEP_STEP_LABEL);
        this.traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
        final GroupByStep groupByStep = (GroupByStep) traversal.asAdmin().getSteps().stream()
                .filter(step -> step.getLabel().equals(this.groupByStepKey))
                .findAny().get();
        this.reduceFunction = groupByStep.getReduceFunction();
        this.mapSupplier = traversal.sideEffects().<Map>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return !stage.equals(Stage.COMBINE);
    }

    @Override
    public void map(Vertex vertex, MapEmitter<Object, Collection> emitter) {
        this.traversal.sideEffects().setLocalVertex(vertex);
        this.traversal.sideEffects().<Map<Object, Collection>>orElse(this.sideEffectKey, Collections.emptyMap()).forEach(emitter::emit);
    }

    @Override
    public void reduce(final Object key, final Iterator<Collection> values, final ReduceEmitter<Object, Object> emitter) {
        final Set set = new BulkSet<>();
        values.forEachRemaining(set::addAll);
        emitter.emit(key, (null == this.reduceFunction) ? set : this.reduceFunction.apply(set));
    }

    @Override
    public Map generateFinalResult(final Iterator<KeyValue<Object, Object>> keyValues) {
        final Map map = this.mapSupplier.get();
        keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
        return map;
    }

    @Override
    public String getMemoryKey() {
        return this.sideEffectKey;
    }

    @Override
    public int hashCode() {
        return (this.getClass().getCanonicalName() + this.sideEffectKey).hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return GraphComputerHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.mapReduceString(this, this.sideEffectKey);
    }

    @Override
    public GroupByMapReduce clone() throws CloneNotSupportedException {
        final GroupByMapReduce clone = (GroupByMapReduce) super.clone();
        clone.traversal = this.traversal.clone();
        return clone;
    }
}