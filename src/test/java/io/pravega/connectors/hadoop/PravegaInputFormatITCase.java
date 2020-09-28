/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.hadoop;

import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import io.pravega.connectors.hadoop.utils.SetupUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.shared.NameUtils.getSegmentNumber;

public class PravegaInputFormatITCase extends ConnectorBaseITCase {

    private static final String TEST_SCOPE = "PravegaInputFormatITCase";
    private static final String TEST_STREAM = "stream";
    private static final int USED_SPACE_PER_INTEGER = 12;
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    private ScheduledExecutorService executor;
    private Controller controller = null;
    private EventStreamWriter<Integer> writer;

    @Before
    public void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices(TEST_SCOPE);
        SETUP_UTILS.createTestStream(TEST_STREAM, 1);
        writer = SETUP_UTILS.getIntegerWriter(TEST_STREAM);

        executor = Executors.newSingleThreadScheduledExecutor();
        controller = new ControllerImpl(ControllerImplConfig.builder()
            .clientConfig(SETUP_UTILS.getClientConfig())
            .retryAttempts(1).build(),
            executor);
    }

    @After
    public void tearDownPravega() throws Exception {
        executor.shutdown();
        SETUP_UTILS.stopAllServices();
    }

    @Test
    public void testGetSplits() throws IOException, ExecutionException, InterruptedException {
        Configuration conf = new Configuration();
        conf = PravegaInputFormat.builder(conf)
            .withScope(TEST_SCOPE)
            .forStream(TEST_STREAM)
            .withURI(SETUP_UTILS.getControllerUri().toString())
            .withDeserializer(IntegerSerializer.class.getName())
            .build();

        addSecurityConfiguration(conf, SETUP_UTILS);
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
        Boolean done = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        Assert.assertTrue(done);
        writeEvents(20, 20);
        splits = inputFormat.getSplits(job);
        Assert.assertEquals(3, splits.size());
        Assert.assertEquals(USED_SPACE_PER_INTEGER * 20 * 2, getTotalUsedSpace(splits));

        done = controller.scaleStream(
                stream,
                Arrays.asList(computeSegmentId(1, 1), computeSegmentId(2, 1)),
                Collections.singletonMap(0.0, 1.0),
                executor).getFuture().get();
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
            Assert.assertEquals(i, getSegmentNumber(p.getSegment().getSegmentId()));
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
