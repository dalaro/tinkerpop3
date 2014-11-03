package com.tinkerpop.gremlin.hmr;

import com.tinkerpop.gremlin.structure.Graph;

// TODO cleanup this copy of Giraph Constants
public class Constants {

    public static final String CONFIGURATION = "configuration";
    public static final String GREMLIN_INPUT_LOCATION = "hmr.gremlin.inputLocation";
    public static final String GREMLIN_OUTPUT_LOCATION = "hmr.gremlin.outputLocation";
    public static final String HMR_INPUT_FORMAT_CLASS = "hmr.inputFormatClass";
    public static final String HMR_OUTPUT_FORMAT_CLASS = "hmr.outputFormatClass";
    public static final String GREMLIN_JARS_IN_DISTRIBUTED_CACHE = "hmr.gremlin.jarsInDistributedCache";
    public static final String HIDDEN_G = Graph.Key.hide("g");
    public static final String HMR_GREMLIN_JOB_PREFIX = "GiraphGremlin: ";
    public static final String GIRAPH_GREMLIN_LIBS = "HMR_GREMLIN_LIBS";
    public static final String DOT_JAR = ".jar";
    public static final String GREMLIN_DERIVE_MEMORY = "hmr.gremlin.deriveMemory";
    public static final String GREMLIN_MEMORY_OUTPUT_FORMAT_CLASS = "hmr.gremlin.memoryOutputFormatClass";
    public static final String HIDDEN_MEMORY = Graph.Key.hide("memory");
    public static final String RUNTIME = Graph.Key.hide("hmr.gremlin.runtime");
    public static final String ITERATION = Graph.Key.hide("hmr.gremlin.iteration");
    public static final String GREMLIN_MEMORY_KEYS = "hmr.gremlin.memoryKeys";
    public static final String MAP_REDUCE_CLASS = "hmr.gremlin.mapReduceClass";
    public static final String GREMLIN_HALT = "hmr.gremlin.halt";
    public static final String MEMORY_MAP = Graph.Key.hide("hmr.gremlin.memoryMap");

}