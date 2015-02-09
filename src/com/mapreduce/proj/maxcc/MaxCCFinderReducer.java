package com.mapreduce.proj.maxcc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxCCFinderReducer extends
        Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

    public void reduce(LongWritable key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
