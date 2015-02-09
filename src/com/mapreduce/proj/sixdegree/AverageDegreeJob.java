package com.mapreduce.proj.sixdegree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by Ray on 4/16/14.
 * Project: MapReduceFinal
 */
public class AverageDegreeJob {
    public static void main(String[] args) throws Exception {

        // initialize a Configuration and job
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: DegreeCount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Compute avg degree ");

        // configure job
        job.setJarByClass(DegreeCount.class);
        job.setMapperClass(AvgDegreeMapper.class);
        job.setReducerClass(AvgDegreeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // specify input path and output path
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // wait for complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
