package com.tinkerpop.gremlin.hmr.process.computer;

import com.tinkerpop.gremlin.hmr.Constants;
import com.tinkerpop.gremlin.hmr.structure.HMRHelper;
import com.tinkerpop.gremlin.hmr.structure.HMRGraph;
import com.tinkerpop.gremlin.process.computer.*;
import org.apache.commons.configuration.FileConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class HMRComputer extends Configured implements GraphComputer, Tool {

    /* Not implemented: private VertexProgram vertexProgram; */
    private final Set<MapReduce> mapReduces;
    private boolean executed = false;
    final HMRImmutableMemory memory = new HMRImmutableMemory();

    private Configuration hadoopConfiguration;

    private final HMRGraph hmrGraph;

    public static final Logger LOGGER = LoggerFactory.getLogger(HMRComputer.class);

    public HMRComputer(final HMRGraph hmrGraph) {
        this.mapReduces = new HashSet<MapReduce>();
        this.hmrGraph = hmrGraph;
        this.hadoopConfiguration = new Configuration();
        final org.apache.commons.configuration.Configuration configuration = hmrGraph.variables().getConfiguration();
        configuration.getKeys().forEachRemaining(key -> hadoopConfiguration.set(key, configuration.getProperty(key).toString()));
    }

    @Override
    public void setConf(Configuration c) {
        //hadoopConfiguration = c;
    }

    @Override
    public GraphComputer isolation(final Isolation isolation) {
        LOGGER.warn("Specifying an isolation level ({}) has no effect on Hadoop MapReduce", isolation);
        return this;
    }

    @Override
    public GraphComputer program(VertexProgram vertexProgram) {
        throw new UnsupportedOperationException("VertexPrograms are not supported on Hadoop MapReduce");
    }

    @Override
    public GraphComputer mapReduce(MapReduce mapReduce) {
        mapReduces.add(mapReduce);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        if (mapReduces.isEmpty())
            throw new IllegalStateException("No MapReduce jobs submitted");

        final long startTime = System.currentTimeMillis();
        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            try {
                final FileSystem fs = FileSystem.get(hadoopConfiguration);
                loadJars(fs);
                fs.delete(new Path(hadoopConfiguration.get(Constants.GREMLIN_OUTPUT_LOCATION)), true);
                ToolRunner.run(this, new String[]{});
                // memory.keys().forEach(k -> LOGGER.error(k + "---" + memory.get(k)));
            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalStateException(e.getMessage(), e);
            }
            memory.complete(System.currentTimeMillis() - startTime);
            return new ComputerResult(HMRHelper.getOutputGraph(hmrGraph), memory);
        });

    }


    private void loadJars(final FileSystem fs) {
        final String giraphGremlinLibsRemote = "hmr-gremlin-libs";
        if (this.hadoopConfiguration.getBoolean(Constants.GREMLIN_JARS_IN_DISTRIBUTED_CACHE, true)) {
            final String giraphGremlinLibsLocal = System.getenv(Constants.GIRAPH_GREMLIN_LIBS);
            if (null == giraphGremlinLibsLocal)
                LOGGER.warn(Constants.GIRAPH_GREMLIN_LIBS + " is not set -- proceeding regardless");
            else {
                final File file = new File(giraphGremlinLibsLocal);
                if (file.exists()) {
                    Arrays.asList(file.listFiles()).stream().filter(f -> f.getName().endsWith(Constants.DOT_JAR)).forEach(f -> {
                        try {
                            final Path jarFile = new Path(fs.getHomeDirectory() + "/" + giraphGremlinLibsRemote + "/" + f.getName());
                            fs.copyFromLocalFile(new Path(f.getPath()), jarFile);
                            try {
                                DistributedCache.addArchiveToClassPath(jarFile, this.hadoopConfiguration, fs);
                            } catch (final Exception e) {
                                throw new RuntimeException(e.getMessage(), e);
                            }
                        } catch (Exception e) {
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                    });
                } else {
                    LOGGER.warn(Constants.GIRAPH_GREMLIN_LIBS + " does not reference a valid directory -- proceeding regardless: " + giraphGremlinLibsLocal);
                }
            }
        }
    }

    @Override
    public int run(String args[]) throws Exception {
        try {
            // Run MapReduce jobs
            for (final MapReduce mapReduce : mapReduces) {
                //MapReduceHelper.executeMapReduceJob(mapReduce, memory, hadoopConfiguration);
                HMRHelper.executeMapReduceJob(mapReduce, memory, hadoopConfiguration);
            }
        } catch (final Throwable t) {
            throw new IllegalStateException(t);
        }
        return 0;
    }

    public static void main(String args[]) throws Exception {
        try {
            final FileConfiguration configuration = new PropertiesConfiguration();
            configuration.load(new File(args[0]));
            final HMRComputer computer = new HMRComputer(HMRGraph.open(configuration));
            computer.program(VertexProgram.createVertexProgram(configuration)).submit().get();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
