package com.mapreduce.proj.conncomp;

import java.io.IOException;

import com.mapreduce.proj.utils.VertexWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CommonSourceSearchReducer extends
        Reducer<LongWritable, VertexWritable, LongWritable, VertexWritable> {

    public static enum UpdateCounter {
        UPDATED
    }

    public void reduce(LongWritable key, Iterable<VertexWritable> values,
                          Context context) throws IOException, InterruptedException {

        VertexWritable realVertex = null;
        LongWritable currentMinimalKey = null;

        for (VertexWritable vertex : values) {
            if (!vertex.isMessage()) {
                // recover the real data structure
                if (realVertex == null) {
                    realVertex = vertex.clone();
                }
            } else {
                // get the minimal source key
                if (currentMinimalKey == null) {
                    currentMinimalKey = new LongWritable(vertex.getVertexId().get());
                } else {
                    if (currentMinimalKey.get() > vertex.getVertexId().get()) {
                        currentMinimalKey = new LongWritable(vertex.getVertexId().get());
                    }
                }
            }
        }

        if(realVertex != null){
            if (currentMinimalKey != null
                    && currentMinimalKey.get() < realVertex.getVertexId().get()) {
                realVertex.setVertexId(currentMinimalKey);
                // update the vertex's minimal source key and force set the activate flag
                // also increment the global counter
                realVertex.setActivated(true);
                context.getCounter(UpdateCounter.UPDATED).increment(1);
            } else {
                // if minimal source is not update, deactivate the vertex
                realVertex.setActivated(false);
            }

        }
        context.write(key, realVertex);
    }

}
