package com.mapreduce.proj.adjlist;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UndirectedEdgeReducer extends
        Reducer<LongWritable, Text, LongWritable, Text> {

    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        String vertices = "";
        for (Text val : values) {
            vertices += val.toString();
        }
        context.write(key, new Text(vertices.substring(0, vertices.length() - 1)));
    }
}
