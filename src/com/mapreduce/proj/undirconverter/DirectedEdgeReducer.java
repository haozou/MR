package com.mapreduce.proj.undirconverter;

import com.mapreduce.proj.utils.LongPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DirectedEdgeReducer extends
        Reducer<LongPair, IntWritable, NullWritable, Text> {
    public void reduce(LongPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int occurrence = 0;
        for(IntWritable iw : values){
            occurrence += iw.get();
        }
        if(occurrence == 2){
            String outputValue = String.valueOf(key.getFirst()) + "," + String.valueOf(key.getSecond());
            context.write(NullWritable.get(), new Text(outputValue));
        }
    }
}
