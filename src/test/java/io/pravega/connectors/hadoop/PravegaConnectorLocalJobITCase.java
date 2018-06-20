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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PravegaConnectorLocalJobITCase {

    private static final String TEST_SCOPE = "PravegaConnectorLocalJobITCase";
    private static final String TEST_STREAM = "stream";
    private static final int NUM_SEGMENTS = 3;
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    private Path outputPath;
    private Job job;
    private FileSystem fs;
    private EventStreamWriter<String> writer;

    @Before
    public void setUp() throws Exception {
        // setup pravega server
        SETUP_UTILS.startAllServices(TEST_SCOPE);
        SETUP_UTILS.createTestStream(TEST_STREAM, NUM_SEGMENTS);
        writer = SETUP_UTILS.getStringWriter(TEST_STREAM);
    }

    @After
    public void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    @Test
    public void testPravegaConnector() throws Exception {
        // TEST 0: without start or end
        writer.writeEvent("begin");
        writer.writeEvent("pravega local job test");
        writer.flush();

        // setup local job runner
        outputPath = new Path("src/test/java/io/pravega/connectors/hadoop/localjobrunnertestdir/");
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "local");
        conf.set("fs.default.name", "file:///");

        fs = FileSystem.getLocal(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job = configureJob(conf, outputPath);
        boolean status = job.waitForCompletion(true);
        Assert.assertTrue(job.isSuccessful());

        File output = new File(outputPath.toUri() + "/");
        Map<String, Integer> counts = getCounts(output);

        Assert.assertEquals(new Integer(1), counts.get("begin"));
        Assert.assertEquals(new Integer(1), counts.get("pravega"));
        Assert.assertEquals(new Integer(1), counts.get("local"));
        Assert.assertEquals(new Integer(1), counts.get("job"));
        Assert.assertEquals(new Integer(1), counts.get("test"));

        // TEST 1: with end position only
        writer.writeEvent("streamcut1 endonly");
        writer.flush();

        fs = FileSystem.getLocal(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        String endPos1 = PravegaInputFormat.fetchLatestPositionsJson(
                SETUP_UTILS.getControllerUri(), TEST_SCOPE, TEST_STREAM);

        // won't be read because it's written after end poisitions are fetched
        writer.writeEvent("onemore");
        writer.flush();

        job = configureJob(conf, outputPath, "", endPos1);
        status = job.waitForCompletion(true);
        Assert.assertTrue(job.isSuccessful());

        output = new File(outputPath.toUri() + "/");
        counts = getCounts(output);

        Assert.assertEquals(null, counts.get("onemore"));
        Assert.assertEquals(new Integer(1), counts.get("begin"));
        Assert.assertEquals(new Integer(1), counts.get("pravega"));
        Assert.assertEquals(new Integer(1), counts.get("local"));
        Assert.assertEquals(new Integer(1), counts.get("job"));
        Assert.assertEquals(new Integer(1), counts.get("test"));
        Assert.assertEquals(new Integer(1), counts.get("streamcut1"));
        Assert.assertEquals(new Integer(1), counts.get("endonly"));

        // TEST 2: with both start and end positions
        writer.writeEvent("streamcut2 startandend");
        writer.flush();

        fs = FileSystem.getLocal(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        String endPos2 = PravegaInputFormat.fetchLatestPositionsJson(
                SETUP_UTILS.getControllerUri(), TEST_SCOPE, TEST_STREAM);

        // won't be read because it's written after end poisitions are fetched
        writer.writeEvent("twomore");
        writer.flush();

        job = configureJob(conf, outputPath, endPos1, endPos2);
        status = job.waitForCompletion(true);
        Assert.assertTrue(job.isSuccessful());

        output = new File(outputPath.toUri() + "/");
        counts = getCounts(output);
        Assert.assertEquals(3, counts.size());
        Assert.assertEquals(new Integer(1), counts.get("streamcut2"));
        Assert.assertEquals(new Integer(1), counts.get("startandend"));
        Assert.assertEquals(new Integer(1), counts.get("onemore"));

        // TEST 3: with start positions only
        writer.writeEvent("streamcut3 startonly");
        writer.flush();

        fs = FileSystem.getLocal(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        job = configureJob(conf, outputPath, endPos2, "");
        status = job.waitForCompletion(true);
        Assert.assertTrue(job.isSuccessful());

        output = new File(outputPath.toUri() + "/");
        counts = getCounts(output);
        Assert.assertEquals(3, counts.size());
        Assert.assertEquals(new Integer(1), counts.get("twomore"));
        Assert.assertEquals(new Integer(1), counts.get("streamcut3"));
        Assert.assertEquals(new Integer(1), counts.get("startonly"));
    }

    private Job configureJob(Configuration conf, Path outputPath, String startPos, String endPos) throws Exception {
        conf.setStrings(PravegaInputFormat.START_POSITIONS, startPos);
        conf.setStrings(PravegaInputFormat.END_POSITIONS, endPos);
        return configureJob(conf, outputPath);
    }

    private Job configureJob(Configuration conf, Path outputPath) throws Exception {
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, TEST_SCOPE);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, TEST_STREAM);
        conf.setStrings(PravegaInputFormat.URI_STRING, SETUP_UTILS.getControllerUri());
        conf.setStrings(PravegaInputFormat.DESERIALIZER, JavaSerializer.class.getName());
        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(PravegaConnectorLocalJobITCase.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SumReducer.class);
        // what we really want to test
        job.setInputFormatClass(PravegaInputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    private Map<String, Integer> getCounts(File outputDir) throws Exception {
        Map<String, Integer> m = new HashMap<String, Integer>();
        Collection<File> files = FileUtils.listFiles(outputDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        for (File f : files) {
            if (f.getName().startsWith("part") && !f.getAbsolutePath().endsWith("crc")) {
                List<String> lines = FileUtils.readLines(f);
                for (String l : lines) {
                    String[] counts = l.split("\\t");
                    m.put(counts[0], Integer.parseInt(counts[1]));
                }
            }
        }
        return m;
    }
}
