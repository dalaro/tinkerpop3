package com.tinkerpop.gremlin.hmr.structure;

import com.google.common.base.Preconditions;
import com.tinkerpop.gremlin.giraph.hdfs.KryoWritableIterator;
import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.hmr.Constants;
import com.tinkerpop.gremlin.hmr.process.computer.HMRMap;
import com.tinkerpop.gremlin.hmr.process.computer.HMRReduce;
import com.tinkerpop.gremlin.hmr.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.Memory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HMRHelper {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(HMRHelper.class);

    public static HMRGraph getOutputGraph(final HMRGraph hmrGraph) {
        final Configuration conf = new BaseConfiguration();
        hmrGraph.variables().getConfiguration().getKeys().forEachRemaining(key -> {
            //try {
            conf.setProperty(key, hmrGraph.variables().getConfiguration().getProperty(key));
            //} catch (Exception e) {
            // do nothing for serialization problems
            //}
        });
        if (hmrGraph.variables().getConfiguration().containsKey(Constants.GREMLIN_OUTPUT_LOCATION)) {
            conf.setProperty(Constants.GREMLIN_INPUT_LOCATION, hmrGraph.variables().getConfiguration().getOutputLocation() + "/" + Constants.HIDDEN_G);
            conf.setProperty(Constants.GREMLIN_OUTPUT_LOCATION, hmrGraph.variables().getConfiguration().getOutputLocation() + "_");
        }
        if (hmrGraph.variables().getConfiguration().containsKey(Constants.HMR_OUTPUT_FORMAT_CLASS)) {
            // TODO: Is this sufficient?
            conf.setProperty(Constants.HMR_INPUT_FORMAT_CLASS, hmrGraph.variables().getConfiguration().getString(Constants.HMR_OUTPUT_FORMAT_CLASS).replace("OutputFormat", "InputFormat"));
        }
        return HMRGraph.open(conf);
    }

    private static final String SEQUENCE_WARNING = "The " + Constants.GREMLIN_MEMORY_OUTPUT_FORMAT_CLASS
            + " is not " + SequenceFileOutputFormat.class.getCanonicalName()
            + " and thus, graph computer memory can not be converted to Java objects";

    private static final Class<? extends Mapper> MAPPER_CLASS = HMRMap.class;
    private static final Class<? extends Reducer> COMBINER_CLASS = HMRReduce.class;
    private static final Class<? extends Reducer> REDUCER_CLASS = HMRReduce.class;

    public static void executeMapReduceJob(final MapReduce mapReduce, final Memory memory, final org.apache.hadoop.conf.Configuration hadoopConfiguration) throws IOException, ClassNotFoundException, InterruptedException {
        final org.apache.hadoop.conf.Configuration newConfiguration = new org.apache.hadoop.conf.Configuration(hadoopConfiguration);
        final BaseConfiguration commonsBaseConfiguration = new BaseConfiguration();
        mapReduce.storeState(commonsBaseConfiguration);
        ConfUtil.mergeApacheIntoHadoopConfiguration(commonsBaseConfiguration, newConfiguration);

        final Path memoryPath;

        if (!mapReduce.doStage(MapReduce.Stage.MAP)) {
            memoryPath = new Path(hadoopConfiguration.get(Constants.GREMLIN_OUTPUT_LOCATION) + "/" + mapReduce.getMemoryKey());
        } else {
            newConfiguration.setClass(Constants.MAP_REDUCE_CLASS, mapReduce.getClass(), MapReduce.class);
            final Job job = new Job(newConfiguration, mapReduce.toString());
            LOGGER.info(Constants.HMR_GREMLIN_JOB_PREFIX + mapReduce.toString());

            // Job jar
            job.setJarByClass(HMRGraph.class);

            // Mapper class (required)
            job.setMapperClass(MAPPER_CLASS);
            LOGGER.debug("Set mapper class: {}", MAPPER_CLASS);

            // Combiner class (optional)
            if (mapReduce.doStage(MapReduce.Stage.COMBINE)) {
                job.setCombinerClass(COMBINER_CLASS);
                LOGGER.debug("Set combiner class: {}", COMBINER_CLASS);
            }

            // Reducer class (optional)
            if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                job.setReducerClass(REDUCER_CLASS);
                LOGGER.debug("Set reducer class: {}", REDUCER_CLASS);
                int nr = 1;
                job.setNumReduceTasks(nr);
                LOGGER.debug("Set number of reduce tasks: {}", nr);
            } else {
                job.setNumReduceTasks(0);
                LOGGER.debug("Disabled the reduce step by setting the number of reduce tasks to zero");
            }

            // KV types
            job.setMapOutputKeyClass(KryoWritable.class);
            job.setMapOutputValueClass(KryoWritable.class);
            job.setOutputKeyClass(KryoWritable.class);
            job.setOutputValueClass(KryoWritable.class);

            // IO formats
            job.setInputFormatClass(newConfiguration.getClass(Constants.HMR_INPUT_FORMAT_CLASS, SequenceFileInputFormat.class, InputFormat.class));
            job.setOutputFormatClass(newConfiguration.getClass(Constants.HMR_OUTPUT_FORMAT_CLASS, SequenceFileOutputFormat.class, OutputFormat.class));

            // if there is no vertex program, then grab the graph from the input location
            Path input = getInputPath(newConfiguration);
            Path output = getOutputPath(newConfiguration);
            Preconditions.checkNotNull(output, "Output path must be non-null");

            final Path graphPath = hadoopConfiguration.get(VertexProgram.VERTEX_PROGRAM, null) != null ? output : input;

            memoryPath = new Path(newConfiguration.get(Constants.GREMLIN_OUTPUT_LOCATION) + "/" + mapReduce.getMemoryKey());

            // necessary if store('a').out.out.store('a') exists twice -- TODO: only need to call the MapReduce once. May want to have a uniqueness critieria on MapReduce.
            if (FileSystem.get(newConfiguration).exists(memoryPath)) {
                FileSystem.get(newConfiguration).delete(memoryPath, true);
            }

            FileInputFormat.setInputPaths(job, graphPath);
            FileOutputFormat.setOutputPath(job, memoryPath);

            job.waitForCompletion(true);
        }

        // if its not a SequenceFile there is no certain way to convert to necessary Java objects.
        // to get results you have to look through HDFS directory structure. Oh the horror.
        if (newConfiguration.getClass(Constants.GREMLIN_MEMORY_OUTPUT_FORMAT_CLASS, SequenceFileOutputFormat.class, OutputFormat.class).equals(SequenceFileOutputFormat.class))
            mapReduce.addResultToMemory(memory, new KryoWritableIterator(hadoopConfiguration, memoryPath));
        else
            LOGGER.warn(SEQUENCE_WARNING);
    }

    private static Path getInputPath(org.apache.hadoop.conf.Configuration conf) {
        return getPath(conf, Constants.GREMLIN_INPUT_LOCATION, null);
    }

    private static Path getOutputPath(org.apache.hadoop.conf.Configuration conf) {
        return getPath(conf, Constants.GREMLIN_OUTPUT_LOCATION, "/" + Constants.HIDDEN_G);
    }

    private static Path getPath(org.apache.hadoop.conf.Configuration conf, String key, String pathSuffix) {
        String str = conf.get(key);
        Preconditions.checkNotNull(str, "Input path " + key + " must be non-null");
        if (null != pathSuffix)
            str += pathSuffix;
        return new Path(str);
    }
}
