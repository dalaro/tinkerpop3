package com.tinkerpop.gremlin.hmr.graphson;

import com.tinkerpop.gremlin.hmr.graphson.GraphSONRecordReader;
import com.tinkerpop.gremlin.hmr.structure.HMRInternalVertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONInputFormat extends FileInputFormat<NullWritable, HMRInternalVertex> implements Configurable {

    private Configuration config;

    @Override
    public RecordReader<NullWritable, HMRInternalVertex> createRecordReader(final InputSplit split, final TaskAttemptContext context) throws IOException, InterruptedException {
        RecordReader<NullWritable, HMRInternalVertex> reader = new GraphSONRecordReader();
        reader.initialize(split, context);
        return reader;
    }

    @Override
    protected boolean isSplitable(final JobContext context, final Path file) {
        return null == new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    }

    @Override
    public void setConf(final Configuration config) {
        this.config = config;
    }

    @Override
    public Configuration getConf() {
        return this.config;
    }
}
