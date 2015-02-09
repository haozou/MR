package com.mapreduce.proj.maxcc;

import com.mapreduce.proj.utils.VertexWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxCCFinderMapper extends
        Mapper<LongWritable, VertexWritable, LongWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, VertexWritable value,
                       Context context) throws IOException, InterruptedException {
        context.write(value.getVertexId(), new IntWritable(1));
    }
}
