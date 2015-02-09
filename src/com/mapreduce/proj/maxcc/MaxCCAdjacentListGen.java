package com.mapreduce.proj.maxcc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class MaxCCAdjacentListGen {
    public static long MAX_CC_MINIMAL_VERTEX_ID;

    public static long getMaxCCMinimalVertex(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line = br.readLine();
        long minimalVertexId = -1;
        long maxConnectedVertexCount = -1;
        String[] fields;
        while (line != null) {
            fields = line.split("\t");
            if(Long.valueOf(fields[1]) > maxConnectedVertexCount){
                maxConnectedVertexCount = Long.valueOf(fields[1]);
                minimalVertexId = Long.valueOf(fields[0]);
            }
            line = br.readLine();
        }
        br.close();
        return minimalVertexId;
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: MaxCCAdjacentListGen <cc_result> <in> <out>");
            System.exit(2);
        }

        MAX_CC_MINIMAL_VERTEX_ID = getMaxCCMinimalVertex(otherArgs[0]);
        Job job = new Job(conf, "Generate the Adjacent List for the Max Connected Component count");
        job.setJarByClass(MaxCCAdjacentListGen.class);

        job.setMapperClass(MaxCCAdjListMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
