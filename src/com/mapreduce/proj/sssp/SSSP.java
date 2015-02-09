package com.mapreduce.proj.sssp;


import com.mapreduce.proj.utils.VertexWithDistanceWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class SSSP extends Configured implements Tool {

	public static String OUT = "outfile";
	public static String IN = "inputlarger";
	public static String source;
	
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: GraphConverter <source vertex> <in> <out>");
			System.exit(3);
		}
		source = otherArgs[0];
		conf.set("source", source);
		// set in and out to args.
		IN = otherArgs[1];
		OUT = otherArgs[2];

		String infile = IN;
		String outputfile = OUT;
		boolean success = false;
		// This job is used to parse the text input file into sequence file
		// in this way, we can read the object from the sequence file which 
		// can improve the performance.
		Job job = new Job(conf, "SSSP");
		job.setJarByClass(SSSP.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(VertexWithDistanceWritable.class);
		job.setMapperClass(TextInputMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(infile));
		FileOutputFormat.setOutputPath(job, new Path(outputfile));

		success = job.waitForCompletion(true);
		
		// this job is used to calculate the shortest path
		int count = 1;
		// global counter used to terminate the loop if no distance update.
		long counter = 1;
		while (counter > 0) {

			infile = outputfile + "/";
			outputfile = OUT + count;

			job = new Job(conf, "SSSP");
			job.setJarByClass(SSSP.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(VertexWithDistanceWritable.class);
			job.setMapperClass(SSSPMapper.class);
			job.setReducerClass(SSSPReducer.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(infile));
			FileOutputFormat.setOutputPath(job, new Path(outputfile));

			success = job.waitForCompletion(true);

			count++;
			counter = job.getCounters()
					.findCounter(SSSPReducer.UpdateCounter.UPDATED).getValue();
		}
		// This job is used to parse the sequence file into text file, in this
		// way, we can read the file
		infile = outputfile + "/";
		outputfile = OUT + System.nanoTime();
		job = new Job(conf, "SSSP");
		job.setJarByClass(SSSP.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(VertexWithDistanceWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(TextOutputMapper.class);
		job.setReducerClass(TextOutputReducer.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(infile));
		FileOutputFormat.setOutputPath(job, new Path(outputfile));
		success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new SSSP(), args));
	}
}
