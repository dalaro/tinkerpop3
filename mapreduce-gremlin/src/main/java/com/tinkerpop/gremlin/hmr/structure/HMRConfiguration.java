package com.tinkerpop.gremlin.hmr.structure;

import com.tinkerpop.gremlin.hmr.Constants;
import com.tinkerpop.gremlin.util.StreamFactory;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

public class HMRConfiguration extends BaseConfiguration implements Serializable, Iterable {

    private Configuration conf;

    public HMRConfiguration() {

    }

    public HMRConfiguration(final Configuration configuration) {
        configuration.getKeys().forEachRemaining(key -> this.setProperty(key, configuration.getProperty(key)));
    }

    public void load(org.apache.hadoop.conf.Configuration c) {
        this.clear();
        Iterator<Map.Entry<String, String>> iter = c.iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String key = entry.getKey();
            String val = entry.getValue();
            setProperty(key, val);
        }
    }

//    public Class<VertexInputFormat> getInputFormat() {
//        try {
//            return (Class) Class.forName(this.getString(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS));
//        } catch (final ClassNotFoundException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//    }
//
//    public void setInputFormat(final Class<VertexInputFormat> inputFormatClass) {
//        this.setProperty(Constants.GIRAPH_VERTEX_INPUT_FORMAT_CLASS, inputFormatClass);
//    }
//
//    public Class<VertexInputFormat> getOutputFormat() {
//        try {
//            return (Class) Class.forName(this.getString(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS));
//        } catch (final ClassNotFoundException e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//    }
//
//    public void setOutputFormat(final Class<VertexOutputFormat> outputFormatClass) {
//        this.setProperty(Constants.GIRAPH_VERTEX_OUTPUT_FORMAT_CLASS, outputFormatClass);
//    }

    public String getInputLocation() {
        return this.getString(Constants.GREMLIN_INPUT_LOCATION);
    }

    public void setInputLocation(final String inputLocation) {
        this.setProperty(Constants.GREMLIN_INPUT_LOCATION, inputLocation);
    }

    public String getOutputLocation() {
        return this.getString(Constants.GREMLIN_OUTPUT_LOCATION);
    }

    public void setOutputLocation(final String outputLocation) {
        this.setProperty(Constants.GREMLIN_OUTPUT_LOCATION, outputLocation);
    }

    @Override
    public Iterator iterator() {
        return StreamFactory.stream(this.getKeys()).map(k -> new Pair(k, this.getProperty(k))).iterator();
    }
}
