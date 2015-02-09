package com.mapreduce.proj.pattern;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// count the triangles for each edge
public class CountTrianglesReducer extends
		Reducer<Text, LongWritable, LongWritable, LongWritable> {
	long count = 0;
	final static LongWritable zero = new LongWritable(0);

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long c = 0, n = 0;
		 // if we get the key with the value 0 and 1, then we can say it is a triangle
		for (LongWritable vs : values) {
			c += vs.get();
			++n;
		}
		if (c != n)
			count += c;
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		if (count > 0)
			context.write(zero, new LongWritable(count));
	}
}
