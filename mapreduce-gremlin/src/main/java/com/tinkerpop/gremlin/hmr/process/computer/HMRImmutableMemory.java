package com.tinkerpop.gremlin.hmr.process.computer;

import com.tinkerpop.gremlin.hmr.Constants;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// TODO code deduplication (this is a copy of GiraphImmutableMemory)
public class HMRImmutableMemory implements Memory {

    private long runtime = 0l;
    private int iteration = -1;
    private final Map<String, Object> memoryMap = new HashMap<>();
    private boolean complete = false;

    @Override
    public Set<String> keys() {
        return this.memoryMap.keySet();
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = (R) this.memoryMap.get(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    @Override
    public void set(final String key, Object value) {
        if (this.complete) throw Memory.Exceptions.memoryCompleteAndImmutable();
        this.memoryMap.put(key, value);
    }

    @Override
    public int getIteration() {
        return this.iteration;
    }

    @Override
    public long getRuntime() {
        return this.runtime;
    }

    protected void complete(final long runtime) {
        this.complete = true;
        this.runtime = runtime;
        if (this.memoryMap.containsKey(Constants.ITERATION))
            this.iteration = (int) this.memoryMap.remove(Constants.ITERATION);
    }

    @Override
    public long incr(final String key, final long delta) {
        throw Memory.Exceptions.memoryCompleteAndImmutable();
    }

    @Override
    public boolean and(final String key, final boolean bool) {
        throw Memory.Exceptions.memoryCompleteAndImmutable();
    }

    @Override
    public boolean or(final String key, final boolean bool) {
        throw Memory.Exceptions.memoryCompleteAndImmutable();
    }

}
