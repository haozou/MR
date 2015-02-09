package com.mapreduce.proj.sssp;

import java.io.IOException;

import com.mapreduce.proj.utils.VertexWithDistanceWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SSSPReducer
		extends
		Reducer<LongWritable, VertexWithDistanceWritable, LongWritable, VertexWithDistanceWritable> {
	// The global counter used to tell the main class whether the vertex's distance is update or not
	public static enum UpdateCounter {
		UPDATED
	}

	public void reduce(LongWritable key,
			Iterable<VertexWithDistanceWritable> values, Context context)
			throws IOException, InterruptedException {

		long minimum = Integer.MAX_VALUE;
		VertexWithDistanceWritable node = new VertexWithDistanceWritable();
		long preId = 0;
		for (VertexWithDistanceWritable vertex : values) {
			// Recover graph structure
			if (!vertex.isMessage()) {
				node = vertex.clone();
			// Look for min distance in list
			} else {
				long distance = vertex.getDistance().get();
				if (distance < minimum) {
					minimum = distance;
					preId = vertex.getPreVertexId().get();
				}
			}
		}
		// Needed to avoid overwriting of source node’s distance
		// Update node’s shortest distance
		if (minimum < node.getDistance().get()) {
			node.setDistance(new LongWritable(minimum));
			node.setActivated(true);
			node.setPreVertexId(new LongWritable(preId));
			context.getCounter(UpdateCounter.UPDATED).increment(1);
		// if the node is not update in the reducer job, we set it 
		// unactivated
		} else
			node.setActivated(false);
		context.write(key, node);
	}
}