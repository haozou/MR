package com.mapreduce.proj.pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class TriangleCounter extends Configured implements Tool
{
    public int run(String[] args) throws Exception
    {
		// job1 is used to generate all the permutation of each node's neighbours
        Job job1 = new Job(getConf());
        job1.setJobName("Triangle Counter");

        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setJarByClass(TriangleCounter.class);
        job1.setMapperClass(ParseLongLongPairsMapper.class);
        job1.setReducerClass(TriadsReducer.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("temp1"));

		// job2 is used to count the triangles
        Job job2 = new Job(getConf());
        job2.setJobName("triangles");

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);

        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setJarByClass(TriangleCounter.class);
        job2.setMapperClass(ParseTextLongPairsMapper.class);
        job2.setReducerClass(CountTrianglesReducer.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path("temp1"));
        FileOutputFormat.setOutputPath(job2, new Path("temp2"));

		//job3 is used to aggregate the job2's triangle counts
        Job job3 = new Job(getConf());
        job3.setJobName("count");

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(LongWritable.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setJarByClass(TriangleCounter.class);
        job3.setMapperClass(ParseTextLongPairsMapper.class);
        job3.setReducerClass(AggregateCountsReducer.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job3, new Path("temp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));


        int ret = job1.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job2.waitForCompletion(true) ? 0 : 1;
        if (ret==0) ret = job3.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TriangleCounter(), args);
        System.exit(res);
    }
}

