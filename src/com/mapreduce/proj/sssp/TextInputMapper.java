package com.mapreduce.proj.sssp;

import java.io.IOException;

import com.mapreduce.proj.utils.VertexWithDistanceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*output the sequence file from the adjacent list of the graph*/
public class TextInputMapper extends
        Mapper<LongWritable, Text, LongWritable, VertexWithDistanceWritable> {
	private LongWritable zero = new LongWritable(0);
	private LongWritable max = new LongWritable(Integer.MAX_VALUE - 1000);
	private String source;
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();  
		source = conf.get("source");
	}
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        VertexWithDistanceWritable vertex = new VertexWithDistanceWritable();
        LongWritable realKey = null;
        String[] values = value.toString().split("\t");
        int currentCount = 0;
        for (String s : values) {
        	// currentCount == 0 means it is the vertex
        	// otherwise it is the vertex that this node points to
            if (currentCount == 0) {
                realKey = new LongWritable(Long.parseLong(s));
                vertex.setVertexId(realKey);
            } else {
                LongWritable temp = new LongWritable(Long.parseLong(s));
                vertex.addAdjacency(temp);
            }
            currentCount++;
        }
        // set the source node distance to zero
        // otherwise set the distance to max
        if (source.equals(values[0])) {
        	vertex.setDistance(zero);
        } else 
        	vertex.setDistance(max);
        // initialize the previous vertex to zero
        vertex.setPreVertexId(zero);
        vertex.setActivated(true);
        context.write(realKey, vertex);
    }

}
