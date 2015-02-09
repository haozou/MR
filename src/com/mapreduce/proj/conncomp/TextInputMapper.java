package com.mapreduce.proj.conncomp;

import java.io.IOException;

import com.mapreduce.proj.utils.VertexWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for the Common Source Search Read job defined in the CommonSourceSearchJob class
 */
public class TextInputMapper extends
        Mapper<LongWritable, Text, LongWritable, VertexWritable> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        VertexWritable vertex = new VertexWritable();
        LongWritable realKey = null;

        // read from the adjacent list and store the information in Vertex type
        for(int i = 0; i < values.length; i ++){
            if(i == 0) {
                realKey = new LongWritable(Long.valueOf(values[i]));
                vertex.setMinimalVertex(realKey);
                vertex.addAdjacency(realKey);
            } else {
                LongWritable lw = new LongWritable(Long.valueOf(values[i]));
                vertex.setMinimalVertex(lw);
                vertex.addAdjacency(lw);
            }
        }

        // emit real data structure of vertex and also emit messages
        context.write(realKey, vertex);
        for (LongWritable adjacency : vertex.getAdjacencies()) {
            context.write(adjacency, vertex.makeMessage());
        }
    }

}
