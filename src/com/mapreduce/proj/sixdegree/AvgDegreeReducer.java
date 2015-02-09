package com.mapreduce.proj.sixdegree;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Ray on 4/16/14.
 * Project: MapReduceFinal
 */
public class AvgDegreeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable iw : values){
            sum += iw.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
