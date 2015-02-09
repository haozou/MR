package com.mapreduce.proj.pattern;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The output key would be the edges and the value would be zero or one
public class ParseTextLongPairsMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    Text outKey = new Text();
    LongWritable outValue = new LongWritable();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {
        String[] nodes = value.toString().split("\t");
        if (nodes.length != 2) {
        	throw new RuntimeException("invalid intermediate line " + value.toString());
        }
        outKey.set(nodes[0]);
        outValue.set(Long.parseLong(nodes[1]));
        context.write(outKey, outValue);
    }
}