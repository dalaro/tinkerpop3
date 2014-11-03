package com.tinkerpop.gremlin.hmr.process.computer;

import com.tinkerpop.gremlin.giraph.process.computer.util.KryoWritable;
import com.tinkerpop.gremlin.hmr.process.computer.util.ConfUtil;
import com.tinkerpop.gremlin.hmr.Constants;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;

public class HMRReduce extends Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable> {

    private MapReduce mapReduce;

    public HMRReduce() {
    }

    @Override
    public void setup(final org.apache.hadoop.mapreduce.Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) {
        try {
            final Class<? extends MapReduce> mapReduceClass = context.getConfiguration().getClass(Constants.MAP_REDUCE_CLASS, MapReduce.class, MapReduce.class);
            final Constructor<? extends MapReduce> constructor = mapReduceClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            this.mapReduce = constructor.newInstance();
            this.mapReduce.loadState(ConfUtil.makeApacheConfiguration(context.getConfiguration()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void reduce(final KryoWritable key, final Iterable<KryoWritable> values, final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) throws IOException, InterruptedException {
        final Iterator<KryoWritable> itty = values.iterator();
        this.mapReduce.reduce(key.get(), new Iterator() {
            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public Object next() {
                return itty.next().get();
            }
        }, new HMRReduceEmitter<>(context));
    }


    public static class HMRReduceEmitter<OK, OV> implements MapReduce.ReduceEmitter<OK, OV> {

        final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context;
        final KryoWritable<OK> keyWritable = new KryoWritable<>();
        final KryoWritable<OV> valueWritable = new KryoWritable<>();

        public HMRReduceEmitter(final Reducer<KryoWritable, KryoWritable, KryoWritable, KryoWritable>.Context context) {
            this.context = context;
        }

        @Override
        public void emit(final OK key, final OV value) {
            this.keyWritable.set(key);
            this.valueWritable.set(value);
            try {
                this.context.write(this.keyWritable, this.valueWritable);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }
}
