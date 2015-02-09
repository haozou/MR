package com.mapreduce.proj.pattern;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// Parse the edges in the file to long long pairs 
public class ParseLongLongPairsMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable>
{
    LongWritable outKey = new LongWritable();
    LongWritable outValue = new LongWritable();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException
    {

        String[] nodes = value.toString().split(","); 
        
        if (nodes.length != 2) {
        	throw new RuntimeException("invalid edge line " + value.toString());
        }
        long n1 = Long.parseLong(nodes[0]);
        long n2 = Long.parseLong(nodes[1]);
		// Since we deal with the undirected graph, we only need to emit the 
		// edges with the smaller node
        if (n1 < n2) {
        	outKey.set(n1);
            outValue.set(n2);
            context.write(outKey, outValue);
        }
        
    }
}