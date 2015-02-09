package com.mapreduce.proj.maxcc;

import com.mapreduce.proj.utils.VertexWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxCCAdjListMapper extends
        Mapper<LongWritable, VertexWritable, LongWritable, Text> {

    public void map(LongWritable key, VertexWritable value,
                    Context context) throws IOException, InterruptedException {
        if (value.getVertexId().get() == MaxCCAdjacentListGen.MAX_CC_MINIMAL_VERTEX_ID) {
            StringBuilder sb = new StringBuilder();
//            if (key.get() == 3)
//                sb.append("0");
//            else
//                sb.append("-1");
//            sb.append("\t");
            for (LongWritable neighborVertex : value.getAdjacencies()) {
                if (neighborVertex.get() != key.get()) {

                    sb.append(neighborVertex.get());
                    sb.append("\t");

                }
            }
            context.write(key, new Text(sb.toString()));
        }
    }
}

