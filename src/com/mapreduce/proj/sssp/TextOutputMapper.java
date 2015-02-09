package com.mapreduce.proj.sssp;

import java.io.IOException;

import com.mapreduce.proj.utils.VertexWithDistanceWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/*output the (vertex	distance	path) to the file from the sequence file
 * that we have already generated*/
public class TextOutputMapper extends
		Mapper<LongWritable, VertexWithDistanceWritable, NullWritable, VertexWithDistanceWritable> {

	@Override
	protected void map(LongWritable key, VertexWithDistanceWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), value);
	}
}