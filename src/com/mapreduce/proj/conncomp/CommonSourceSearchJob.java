package com.mapreduce.proj.conncomp;

import java.io.IOException;
import com.mapreduce.proj.utils.VertexWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * This class is used to do the Common Source Search to find out all the connected
 * component from the given undirected graph
 */
public class CommonSourceSearchJob {

    private static final String DEFAULT_FILES_IN = "";
    private static final String DEFAULT_FILES_OUT = "";

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        String inPath = null;
        String outPath = null;
        if (args.length > 0) {
            inPath = args[0];
            if (args.length > 1) {
                outPath = args[1];
            }
        }


        int roundNum = 1;
        Configuration conf = new Configuration();

        // this job read the text format of the undirected graph adjacent list
        // and output the key-value pairs in vertex format for the latter MapReduce
        // iterations
        Job job = new Job(conf, "Common Source Search Read");

        job.setJarByClass(CommonSourceSearchJob.class);
        job.setMapperClass(TextInputMapper.class);
        job.setReducerClass(TextInputReducer.class);

        Path inputPath = new Path(inPath == null ? DEFAULT_FILES_IN : inPath);
        Path outputPath = new Path(outPath == null ? DEFAULT_FILES_OUT + 1: outPath + 1);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(VertexWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VertexWritable.class);

        job.waitForCompletion(true);

        long counter = job.getCounters()
                .findCounter(TextInputReducer.UpdateCounter.UPDATED).getValue();
        roundNum++;

        // Iteratively run the Common Source Search Compute job to
        // update the vertices' minimal common source until all the
        // vertices in the graph cannot be updated
        while (counter > 0) {
            conf = new Configuration();
            job = new Job(conf);
            job.setJobName("Common Source Search Compute " + roundNum);
            job.setJarByClass(CommonSourceSearchJob.class);
            job.setMapperClass(CommonSourceSearchMapper.class);
            job.setReducerClass(CommonSourceSearchReducer.class);

            inputPath = new Path(outPath + (roundNum - 1) + "/");
            outputPath = new Path(outPath + roundNum);
            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(VertexWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(VertexWritable.class);

            job.waitForCompletion(true);
            roundNum++;
            counter = job.getCounters()
                    .findCounter(CommonSourceSearchReducer.UpdateCounter.UPDATED).getValue();
        }
    }
}
