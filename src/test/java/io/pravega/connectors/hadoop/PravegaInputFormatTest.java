/*
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
import io.pravega.connectors.hadoop.PravegaInputFormat;
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import io.pravega.connectors.hadoop.utils.SetupUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PravegaInputFormatTest {

    private static final String scope = "scope";
    private static final String stream = "stream";
    private static final int numSegments = 3;
    private static final int numEvents = 20;

    /**
     * Setup utility
     */
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    @Before
    public void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices(this.scope);
        SETUP_UTILS.createTestStream(this.stream, this.numSegments);
        EventStreamWriter<Integer> writer = SETUP_UTILS.getIntegerWriter(this.stream);
        for (int i = 0; i < this.numEvents; i++) {
            CompletableFuture future = writer.writeEvent(i);
            future.get();
        }
    }

    @After
    public void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testGetSplits() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, this.scope);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, this.stream);
        conf.setStrings(PravegaInputFormat.URI_STRING, SETUP_UTILS.getControllerUri());
        conf.setStrings(PravegaInputFormat.DESERIALIZER, IntegerSerializer.class.getName());
        Job job = new Job(conf);

        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat<>();
        List<InputSplit> splits = inputFormat.getSplits(job);
        Assert.assertEquals(this.numSegments, splits.size());
        int totalLen, totalEndOffset;
        totalLen = totalEndOffset = 0;
        for (int i = 0; i < splits.size(); i++) {
            PravegaInputSplit p = (PravegaInputSplit) (splits.get(i));
            Assert.assertEquals(i, p.getSegment().getSegmentNumber());
            Assert.assertEquals(0, p.getStartOffset());
            totalLen += p.getEndOffset();
            totalEndOffset += p.getLength();
        }
        Assert.assertEquals(this.numEvents * 12, totalLen);
        Assert.assertEquals(this.numEvents * 12, totalEndOffset);
    }
}
