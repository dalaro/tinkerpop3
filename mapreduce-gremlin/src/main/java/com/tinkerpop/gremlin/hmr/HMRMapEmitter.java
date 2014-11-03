package com.tinkerpop.gremlin.hmr;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.OutputFormat;

public class HMRMapEmitter<K, V> implements MapReduce.MapEmitter<K, V> {

    private OutputFormat<K, V> outputFormat;

    @Override
    public void emit(K key, V value) {
       throw new UnsupportedOperationException("Not yet implemented"); // TODO implement
    }
}
