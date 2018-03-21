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
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import io.pravega.connectors.hadoop.utils.SetupUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class PravegaInputRecordReaderITCase {

    private static final String TEST_SCOPE = "PravegaInputRecordReaderITCase";
    private static final String TEST_STREAM_1 = "stream1";
    private static final String TEST_STREAM_2 = "stream2";
    private static final int NUM_EVENTS = 20;

    /**
     * Setup utility
     */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @Before
    public void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices(TEST_SCOPE);
    }

    @After
    public void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testReadFromSingleSegment() throws Exception {
        SETUP_UTILS.createTestStream(TEST_STREAM_1, 1);
        EventStreamWriter<Integer> writer = SETUP_UTILS.getIntegerWriter(TEST_STREAM_1);
        for (int i = 0; i < NUM_EVENTS; i++) {
            CompletableFuture future = writer.writeEvent(i);
            future.get();
        }

        Configuration conf = getConfiguration(TEST_STREAM_1);
        Job job = new Job(conf);

        // get an InputSplit
        InputFormat<EventKey, Integer> inputFormat = new PravegaInputFormat<>();
        List<InputSplit> splits = inputFormat.getSplits(job);
        Assert.assertEquals(1, splits.size());

        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<EventKey, Integer> r = inputFormat.createRecordReader(splits.get(0), context);
        r.initialize(splits.get(0), context);

        for (int i = 0; i < NUM_EVENTS; i++) {
            Assert.assertTrue(r.nextKeyValue());
            Assert.assertEquals(i * 12, r.getCurrentKey().getOffset());
            Assert.assertTrue(i == r.getCurrentValue());
        }
        Assert.assertFalse(r.nextKeyValue());

        r.close();
    }

    @Test
    public void testReadFromMultiSegments() throws Exception {
        SETUP_UTILS.createTestStream(TEST_STREAM_2, 2);
        EventStreamWriter<Integer> writer = SETUP_UTILS.getIntegerWriter(TEST_STREAM_2);
        for (int i = 0; i < NUM_EVENTS; i++) {
            CompletableFuture future = writer.writeEvent(i);
            future.get();
        }

        Configuration conf = getConfiguration(TEST_STREAM_2);
        Job job = new Job(conf);

        // get an InputSplit
        InputFormat<EventKey, Integer> inputFormat = new PravegaInputFormat<>();
        List<InputSplit> splits = inputFormat.getSplits(job);
        Assert.assertEquals(2, splits.size());

        ArrayList<Integer> nums = new ArrayList<Integer>();
        for (InputSplit split : splits) {
            TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
            RecordReader<EventKey, Integer> r = inputFormat.createRecordReader(split, context);
            r.initialize(split, context);
            while (r.nextKeyValue()) {
                nums.add(r.getCurrentValue());
            }
            r.close();
        }
        Assert.assertEquals(NUM_EVENTS, nums.size());
    }

    private Configuration getConfiguration(final String stream) {
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, TEST_SCOPE);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, stream);
        conf.setStrings(PravegaInputFormat.URI_STRING, SETUP_UTILS.getControllerUri());
        conf.setStrings(PravegaInputFormat.DESERIALIZER, IntegerSerializer.class.getName());
        return conf;
    }

}
