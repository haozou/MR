package com.mapreduce.proj.sssp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import com.mapreduce.proj.utils.VertexWithDistanceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TextOutputReducer extends
		Reducer<NullWritable, VertexWithDistanceWritable, LongWritable, Text> {
	public void reduce(NullWritable key,
			Iterable<VertexWithDistanceWritable> values, Context context)
			throws IOException, InterruptedException {
		HashMap<Long, String> previous = new HashMap<Long, String>();

		for (VertexWithDistanceWritable v : values) {
			previous.put(v.getVertexId().get(), v.getPreVertexId().get() + " " + v.getDistance().get());
		}
		Configuration conf = context.getConfiguration();
		String source = conf.get("source");

		for (Entry<Long, String> v : previous.entrySet()) {
			StringBuilder sb = new StringBuilder();
			long vKey = v.getKey();
			long pre = vKey;
			String[] pre_with_dis = v.getValue().split(" "); 
			sb.append(pre_with_dis[1]);
			sb.append("\t");
			// find the path to the source node
			while (true) {
				if (Long.parseLong(source) == pre) {
					break;
				}
				pre = Long.parseLong(previous.get(pre).split(" ")[0]);
				sb.append(pre);
				if (pre != Long.parseLong(source))
					sb.append(",");
			}
			context.write(new LongWritable(vKey), new Text(sb.toString()));
		}

	}
}
