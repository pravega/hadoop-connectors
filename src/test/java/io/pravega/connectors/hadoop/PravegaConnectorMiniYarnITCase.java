/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.hadoop;


import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.hadoop.utils.SetupUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;


public class PravegaConnectorMiniYarnITCase {

    private static final String TEST_SCOPE = "PravegaConnectorMiniYarnITCase";
    private static final String TEST_STREAM = "stream";
    private static final int NUM_SEGMENTS = 3;
    private static final int NUM_NODES = 2;
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    private Path outputPath;
    private Job job;
    private MiniYARNCluster yarnCluster;
    private MiniDFSCluster dfsCluster;

    @Before
    public void setUp() throws Exception {
        // setup pravega server
        SETUP_UTILS.startAllServices(TEST_SCOPE);
        SETUP_UTILS.createTestStream(TEST_STREAM, NUM_SEGMENTS);
        EventStreamWriter<String> writer = SETUP_UTILS.getStringWriter(TEST_STREAM);
        writer.writeEvent("pravega test");
        writer.writeEvent("pravega job test");
        writer.writeEvent("pravega local job test");

        // setup mini dfs cluster
        YarnConfiguration conf = new YarnConfiguration();

        conf.setBoolean("yarn.is.minicluster", true);
        dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_NODES).build();
        dfsCluster.waitActive();

        // setup miniyarn server
        yarnCluster = new MiniYARNCluster(getClass().getSimpleName(), NUM_NODES, 1, 1);
        yarnCluster.init(conf);
        yarnCluster.start();

        // setup job
        outputPath = new Path("miniclustertestdir/");
        FileSystem fs = dfsCluster.getFileSystem();
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        job = configureJob(yarnCluster.getConfig(), outputPath);
    }

    @After
    public void tearDownPravega() throws Exception {
        if (yarnCluster != null) {
            yarnCluster.stop();
        }
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testPravegaConnector() throws Exception {
        boolean status = job.waitForCompletion(true);
        Assert.assertTrue(job.isSuccessful());

        Map<String, Integer> counts = getCounts(dfsCluster.getFileSystem(), outputPath);
        Assert.assertEquals(new Integer(3), counts.get("pravega"));
        Assert.assertEquals(new Integer(1), counts.get("local"));
        Assert.assertEquals(new Integer(2), counts.get("job"));
        Assert.assertEquals(new Integer(3), counts.get("test"));
    }

    private Job configureJob(Configuration conf, Path outputPath) throws Exception {
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, TEST_SCOPE);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, TEST_STREAM);
        conf.setStrings(PravegaInputFormat.URI_STRING, SETUP_UTILS.getControllerUri());
        conf.setStrings(PravegaInputFormat.DESERIALIZER, JavaSerializer.class.getName());
        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(getClass());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);
        // what we really want to test
        job.setInputFormatClass(PravegaInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    private Map<String, Integer> getCounts(FileSystem fs, Path outputPath) throws Exception {
        Map<String, Integer> m = new HashMap<String, Integer>();
        FileStatus[] status = fs.listStatus(outputPath, new OutputFilesFilter());
        for (FileStatus f : status) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(f.getPath())));
            String line = br.readLine();
            while (line != null) {
                String[] counts = line.split("\\t");
                m.put(counts[0], Integer.parseInt(counts[1]));
                line = br.readLine();
            }
        }
        return m;
    }
}
