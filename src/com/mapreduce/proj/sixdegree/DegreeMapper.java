package com.mapreduce.proj.sixdegree;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DegreeMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] nodes = value.toString().split("\t");
        int distance = Integer.valueOf(nodes[1]);
        context.write(new IntWritable(distance), new IntWritable(1));
    }
}
