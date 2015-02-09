package com.mapreduce.proj.undirconverter;

import com.mapreduce.proj.utils.LongPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DirectedEdgeMapper extends
        Mapper<Object, Text, LongPair, IntWritable> {

    LongPair outputKey;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] nodes = value.toString().split(",");
        long srcVertex = Long.valueOf(nodes[0]);
        long tgtVertex = Long.valueOf(nodes[1]);


        if(srcVertex < tgtVertex){
            outputKey = new LongPair(srcVertex, tgtVertex);
        }
        else{
            outputKey = new LongPair(tgtVertex, srcVertex);
        }
        context.write(outputKey, new IntWritable(1));
    }
}
