package com.mapreduce.proj.conncomp;

import java.io.IOException;

import com.mapreduce.proj.utils.VertexWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * This is the Mapper class for the Common Source Search Compute job
 */
public class CommonSourceSearchMapper extends
        Mapper<LongWritable, VertexWritable, LongWritable, VertexWritable> {

    public void map(LongWritable key, VertexWritable value, Context context)
            throws IOException, InterruptedException {
        // emit the real data structure of the vertex
        context.write(key, value);
        // only emit the vertex with the activated flag is true
        if (value.isActivated()) {
            VertexWritable vertex = new VertexWritable();
            for (LongWritable adjacency : value.getAdjacencies()) {
                if (adjacency.get() != value.getVertexId().get()) {
                    vertex.setVertexId(value.getVertexId());
                    vertex.setAdjacencies(null);
                    // emit a "message"
                    context.write(adjacency, vertex);
                }
            }
        }
    }

}
