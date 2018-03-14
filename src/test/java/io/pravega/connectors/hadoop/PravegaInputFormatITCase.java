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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import io.pravega.connectors.hadoop.utils.SetupUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class PravegaInputFormatITCase {

    private static final String TEST_SCOPE = "PravegaInputFormatITCase";
    private static final String TEST_STREAM = "stream";
    private static final int USED_SPACE_PER_INTEGER = 12;
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    private ScheduledExecutorService executor;
    private ControllerImpl controller = null;
    private EventStreamWriter<Integer> writer;

    @Before
    public void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices(TEST_SCOPE);
        SETUP_UTILS.createTestStream(TEST_STREAM, 1);
        writer = SETUP_UTILS.getIntegerWriter(TEST_STREAM);

        executor = Executors.newSingleThreadScheduledExecutor();
        controller = new ControllerImpl(URI.create("tcp://localhost:" + SETUP_UTILS.getControllerPort()),
                ControllerImplConfig.builder().retryAttempts(1).build(), executor);
    }

    @After
    public void tearDownPravega() throws Exception {
        executor.shutdown();
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testGetSplits() throws IOException, ExecutionException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, TEST_SCOPE);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, TEST_STREAM);
        conf.setStrings(PravegaInputFormat.URI_STRING, SETUP_UTILS.getControllerUri());
        conf.setStrings(PravegaInputFormat.DESERIALIZER, IntegerSerializer.class.getName());
        Job job = new Job(conf);

        writeEvents(20, 0);
        InputFormat<EventKey, Integer> inputFormat = new PravegaInputFormat<>();
        List<InputSplit> splits = inputFormat.getSplits(job);
        Assert.assertEquals(1, splits.size());
        Assert.assertEquals(USED_SPACE_PER_INTEGER * 20, getTotalUsedSpace(splits));

        Stream stream = new StreamImpl(TEST_SCOPE, TEST_STREAM);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean done = controller.scaleStream(stream, Collections.singletonList(0), map, executor).getFuture().get();
        Assert.assertTrue(done);
        writeEvents(20, 20);
        splits = inputFormat.getSplits(job);
        Assert.assertEquals(3, splits.size());
        Assert.assertEquals(USED_SPACE_PER_INTEGER * 20 * 2, getTotalUsedSpace(splits));

        map = new HashMap<>();
        map.put(0.0, 1.0);
        ArrayList<Integer> seal = new ArrayList<>();
        seal.add(1);
        seal.add(2);
        done = controller.scaleStream(stream, Collections.unmodifiableList(seal), map, executor).getFuture().get();
        Assert.assertTrue(done);
        writeEvents(20, 40);
        splits = inputFormat.getSplits(job);
        Assert.assertEquals(4, splits.size());
        Assert.assertEquals(USED_SPACE_PER_INTEGER * 20 * 3, getTotalUsedSpace(splits));
    }

    private int getTotalUsedSpace(List<InputSplit> splits) throws IOException, InterruptedException {
        int totalLength = 0;
        for (int i = 0; i < splits.size(); i++) {
            PravegaInputSplit p = (PravegaInputSplit) (splits.get(i));
            Assert.assertEquals(i, p.getSegment().getSegmentNumber());
            totalLength += p.getLength();
        }
        return totalLength;
    }

    private void writeEvents(int n, int start) throws ExecutionException, InterruptedException {
        for (int i = start; i < start + n; i++) {
            writer.writeEvent(i).get();
        }
    }
}
