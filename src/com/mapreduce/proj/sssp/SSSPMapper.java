package com.mapreduce.proj.sssp;

import java.io.IOException;

import com.mapreduce.proj.utils.VertexWithDistanceWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

// The major mapper task used to emit the vertex object and all the distance from the source 
// to all its neighbours.
public class SSSPMapper extends
		Mapper<LongWritable, VertexWithDistanceWritable, LongWritable, VertexWithDistanceWritable> {
	
	public void map(LongWritable key, VertexWithDistanceWritable value,
			Context context) throws IOException, InterruptedException {
		// emit the vertex structure
		context.write(key, value);
		// if  the vertex is not update in previous reducer job, we don't need to
		// emit the distance.
		if (value.isActivated()) {
			VertexWithDistanceWritable vertex = new VertexWithDistanceWritable();
			LongWritable distanceadd = new LongWritable();
			// set the new distance.
			distanceadd.set(value.getDistance().get() + 1);
			// emit all the distance to the neighbours
			for (LongWritable vertexId : value.getAdjacencies()) {
				vertex.setVertexId(vertexId);
				vertex.setDistance(distanceadd);
				vertex.setPreVertexId(key);
				vertex.setAdjacencies(null);
				context.write(vertex.getVertexId(), vertex);
			}
		}
	}
}
