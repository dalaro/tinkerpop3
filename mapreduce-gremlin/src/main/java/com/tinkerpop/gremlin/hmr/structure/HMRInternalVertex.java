package com.tinkerpop.gremlin.hmr.structure;

import com.tinkerpop.gremlin.hmr.process.computer.HMRImmutableMemory;
import com.tinkerpop.gremlin.process.computer.VertexProgram;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

public class HMRInternalVertex implements Configurable {

    //TODO: Dangerous that the underlying TinkerGraph Vertex can have edges written to it.

    private static final String VERTEX_ID = Graph.Key.hide("vertexId");
    private VertexProgram vertexProgram;
    private TinkerGraph tinkerGraph;
    private TinkerVertex tinkerVertex;
    private HMRImmutableMemory memory;
    private Configuration hadoopConfiguration;

    public HMRInternalVertex() {

    }

    public HMRInternalVertex(final TinkerVertex tinkerVertex) {
        this.tinkerVertex = tinkerVertex;
        this.tinkerVertex.graph().variables().set(VERTEX_ID, this.tinkerVertex.id());
    }

    public TinkerVertex getTinkerVertex() {
        return this.tinkerVertex;
    }
//
//    @Override
//    public void compute(final Iterable<KryoWritable> messages) {
//        if (null == tinkerVertex)
//            inflateTinkerVertex();
//        if (null == vertexProgram)
//            vertexProgram = VertexProgram.createVertexProgram(ConfUtil.makeApacheConfiguration(getConf()));
//        if (null == memory)
//            memory = new HMRMemory(this, vertexProgram);
//
//        if (!(Boolean) ((RuleWritable) this.getAggregatedValue(Constants.GREMLIN_HALT)).getObject())
//            this.vertexProgram.execute(this.tinkerVertex, new GiraphMessenger(this, messages), this.memory);  // TODO provide a wrapper around TinkerVertex for Edge and non-ComputeKeys manipulation
//        else if (this.getConf().getBoolean(Constants.GREMLIN_DERIVE_MEMORY, false)) {
//            final Map<String, Object> memoryMap = new HashMap<>(this.memory.asMap());
//            memoryMap.put(Constants.ITERATION, this.memory.getIteration() - 1);
//            this.tinkerVertex.singleProperty(Constants.MEMORY_MAP, memoryMap);
//        }
//    }

    ///////////////////////////////////////////////

//    private Text deflateTinkerVertex() {
//        try {
//            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
//            final KryoWriter writer = KryoWriter.build().create();
//            writer.writeGraph(bos, this.tinkerGraph);
//            bos.flush();
//            bos.close();
//            return new Text(bos.toByteArray());
//        } catch (final IOException e) {
//            throw new IllegalStateException(e.getMessage(), e);
//        }
//    }
//
//    private void inflateTinkerVertex() {
//        try {
//            final ByteArrayInputStream bis = new ByteArrayInputStream(this.getValue().getBytes());
//            final KryoReader reader = KryoReader.build().create();
//            this.tinkerGraph = TinkerGraph.open();
//            reader.readGraph(bis, this.tinkerGraph);
//            bis.close();
//            this.tinkerVertex = (TinkerVertex) this.tinkerGraph.v(this.tinkerGraph.variables().get(VERTEX_ID).get());
//        } catch (final Exception e) {
//            throw new IllegalStateException(e.getMessage(), e);
//        }
//    }

    @Override
    public void setConf(Configuration entries) {
        hadoopConfiguration = entries;
    }

    @Override
    public Configuration getConf() {
        return hadoopConfiguration;
    }
}
