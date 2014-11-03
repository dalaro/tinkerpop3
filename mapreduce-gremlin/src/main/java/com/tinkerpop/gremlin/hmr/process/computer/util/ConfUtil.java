package com.tinkerpop.gremlin.hmr.process.computer.util;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

// TODO this is a copy of Giraph's ConfUtil and should be deduplicated
// There are some Giraph specific references in the getInputFormatFromVertexInputFormat method
public class ConfUtil {

    public static org.apache.commons.configuration.Configuration makeApacheConfiguration(final Configuration hadoopConfiguration) {
        final BaseConfiguration apacheConfiguration = new BaseConfiguration();
        hadoopConfiguration.iterator().forEachRemaining(e -> apacheConfiguration.setProperty(e.getKey(), e.getValue()));
        return apacheConfiguration;
    }

    public static Configuration makeHadoopConfiguration(final org.apache.commons.configuration.Configuration apacheConfiguration) {
        final Configuration hadoopConfiguration = new Configuration();
        apacheConfiguration.getKeys().forEachRemaining(key -> {
            final Object object = apacheConfiguration.getProperty(key);
            hadoopConfiguration.set(key, object.toString());
        });
        return hadoopConfiguration;
    }

    public static void mergeApacheIntoHadoopConfiguration(final org.apache.commons.configuration.Configuration apacheConfiguration, final Configuration hadoopConfiguration) {
        apacheConfiguration.getKeys().forEachRemaining(key -> {
            final Object object = apacheConfiguration.getProperty(key);
            hadoopConfiguration.set(key, object.toString());
        });
    }

//    public static Class<InputFormat> getInputFormatFromVertexInputFormat(final Class<VertexInputFormat> vertexInputFormatClass) {
//        try {
//            if (GiraphGremlinInputFormat.class.isAssignableFrom(vertexInputFormatClass))
//                return (((GiraphGremlinInputFormat) vertexInputFormatClass.getConstructor().newInstance()).getInputFormatClass());
//        } catch (final Exception e) {
//            throw new RuntimeException(e.getMessage(), e);
//        }
//        throw new IllegalStateException("The provided VertexInputFormatClass is not a GiraphGremlinInputFormat");
//    }

}