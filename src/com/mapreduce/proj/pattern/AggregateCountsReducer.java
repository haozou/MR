package com.mapreduce.proj.pattern;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// Just aggregate the previous output 
public class AggregateCountsReducer extends
		Reducer<Text, LongWritable, LongWritable, LongWritable> {
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable vs : values) {
			sum += vs.get();
		}
		context.write(new LongWritable(sum), null);
	}
}
