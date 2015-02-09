package com.mapreduce.proj.pattern;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//produce all the original  edges and all the permutation  for each node’s neighbours
public class TriadsReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable>
{
    Text outKey = new Text();
    final static LongWritable zero = new LongWritable((byte)0);
    final static LongWritable one = new LongWritable((byte)1);
    

    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException
    {
    	Vector<Long> vArray = new Vector<Long>();
		// emit the original edges with the edges as the key and the zero as the value
        for (LongWritable vs : values ) {

            long n = vs.get();
            vArray.add(n);

            outKey.set(key.toString() + "," + Long.toString(n));
            context.write(outKey, zero);
        }
		// emit all the permutation for the all the node's neighbours and 
		// emit the edges as the key and the one as the value
        for (int i=0; i<vArray.size(); ++i) {
            for (int j=i+1; j<vArray.size(); ++j) {
				// we always emit the edges with the smaller node in front
            	if (vArray.elementAt(i) < vArray.elementAt(j)) {
            		outKey.set(Long.toString(vArray.elementAt(i)) + "," + Long.toString(vArray.elementAt(j)));
            	} else {
            		outKey.set(Long.toString(vArray.elementAt(j)) + "," + Long.toString(vArray.elementAt(i)));
            	}
                context.write(outKey, one);
            }
        }
    }
}
