package com.semijoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SemiJoin {

    public static void main(String[] args) throws Exception {
        // Check if the correct number of arguments is passed
        if (args.length != 2) {
            System.err.println("Usage: SemiJoin <input path> <output path>");
            System.exit(-1);
        }

        // Configuration setup
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Semi Join");
        job.setJarByClass(SemiJoin.class);
        
        // Set Mapper and Reducer classes
        job.setMapperClass(SemiJoinMapper.class);
        job.setReducerClass(SemiJoinReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Dataset A + Dataset B combined file
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

