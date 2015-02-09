package com.mapreduce.proj.sixdegree;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Ray on 4/16/14.
 * Project: MapReduceFinal
 */
public class AvgDegreeMapper  extends Mapper<Object, Text, IntWritable, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] nodes = value.toString().split("\t");
        int distance = Integer.valueOf(nodes[1]);
        context.write(new IntWritable(Integer.valueOf(1)), new IntWritable(1));
    }

}
