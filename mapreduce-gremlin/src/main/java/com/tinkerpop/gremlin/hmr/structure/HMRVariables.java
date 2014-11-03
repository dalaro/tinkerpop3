package com.tinkerpop.gremlin.hmr.structure;

import com.tinkerpop.gremlin.hmr.Constants;
import com.tinkerpop.gremlin.hmr.structure.HMRConfiguration;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HMRVariables implements Graph.Variables {

    private final Map<String, Object> variables = new HashMap<>();

    public HMRVariables(final HMRConfiguration configuration) {
        this.variables.put(Constants.CONFIGURATION, configuration);
    }

    @Override
    public Set<String> keys() {
        return this.variables.keySet();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.variables.get(key));
    }

    @Override
    public void remove(final String key) {
        this.variables.remove(key);
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.variables.put(key, value);
    }

    public HMRConfiguration getConfiguration() {
        return this.<HMRConfiguration>get(Constants.CONFIGURATION).get();
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
