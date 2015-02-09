package com.mapreduce.proj.conncomp;

import com.mapreduce.proj.utils.VertexWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This is the reducer class for the Common Source Search Read job
 */
public class TextInputReducer extends
        Reducer<LongWritable, VertexWritable, LongWritable, VertexWritable> {

    public static enum UpdateCounter {
        UPDATED
    }

    public void reduce(LongWritable key, Iterable<VertexWritable> values,
                          Context context) throws IOException, InterruptedException {

        VertexWritable realVertex = null;
        for (VertexWritable vertex : values) {
            // recover the vertex structure
            if (!vertex.isMessage()) {
                realVertex = vertex.clone();
            }
        }

        // set activated to true for the next MapReduce iterations
        // also update the vertex id and global counters
        if(realVertex != null){
            realVertex.setActivated(true);
            realVertex.setVertexId(realVertex.getAdjacencies().first());
            if (key.get() < realVertex.getVertexId().get())
                realVertex.setVertexId(key);
            context.getCounter(UpdateCounter.UPDATED).increment(1);
        }

        context.write(key, realVertex);
    }
}
