/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.examples.hadoop;

import io.pravega.connectors.hadoop.PravegaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 4) {
            System.err.println("Usage: WordCount <url> <scope> <stream> <out>");
            System.exit(2);
        }

        conf.setStrings(PravegaInputFormat.URI_STRING, remainingArgs[0]);
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, remainingArgs[1]);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, remainingArgs[2]);

        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);

        job.setInputFormatClass(PravegaInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[3]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
