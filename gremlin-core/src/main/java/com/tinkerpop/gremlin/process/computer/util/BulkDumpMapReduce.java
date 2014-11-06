package com.tinkerpop.gremlin.process.computer.util;

import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

public class BulkDumpMapReduce implements MapReduce {

    private static final String SIDE_EFFECT_KEY = "titan.bulkDump.sideEffectKey";

    private final GraphSONWriter graphSONWriter = GraphSONWriter.build().create();

    @Override
    public void map(Vertex vertex, MapEmitter emitter) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            graphSONWriter.writeVertex(baos, vertex, Direction.BOTH); // TODO IN or OUT instead of BOTH?
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        emitter.emit(baos.toString());
    }

    @Override
    public boolean doStage(Stage stage) {
        return stage.equals(Stage.MAP);
    }

    @Override
    public String getMemoryKey() {
        return SIDE_EFFECT_KEY;
    }

    @Override
    public Object generateFinalResult(Iterator iterator) {
        return null;
    }
}
