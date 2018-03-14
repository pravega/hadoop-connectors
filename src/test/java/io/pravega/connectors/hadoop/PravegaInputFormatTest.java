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

import io.pravega.client.ClientFactory;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ClientFactory.class)
public class PravegaInputFormatTest {

    private static final String TEST_SCOPE = "PravegaInputFormatTest";
    private static final String TEST_STREAM = "stream";
    private static final String TEST_URI = "tcp://127.0.0.1:9090";

    @Before
    public void setupTests() throws Exception {
        mockClientFactoryStatic(TEST_SCOPE, TEST_URI);
    }

    @Test
    public void testGetSplits() throws Exception {
        JobContext ctx = getJobContext();
        Configuration c = ctx.getConfiguration();
        Assert.assertNotNull(c);
        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat();
        List<InputSplit> splits = inputFormat.getSplits(ctx);
        Assert.assertEquals(splits.size(), 3);
        int i = 0;
        for (InputSplit s : splits) {
            Assert.assertTrue(s instanceof PravegaInputSplit);
            PravegaInputSplit ps = (PravegaInputSplit) s;
            i++;
            Assert.assertTrue(0 == ps.compareTo(new PravegaInputSplit(new Segment(TEST_SCOPE, TEST_STREAM, i), 0, 100 * i)));
        }
    }

    @Test
    public void testCreateRecordReader() throws IOException, InterruptedException {
        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat();
        RecordReader<?, ?> reader = inputFormat.createRecordReader(null, null);
        Assert.assertTrue(reader instanceof PravegaInputRecordReader);
    }

    private JobContext getJobContext() throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, TEST_SCOPE);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, TEST_STREAM);
        conf.setStrings(PravegaInputFormat.URI_STRING, TEST_URI);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, IntegerSerializer.class.getName());
        Job mockJob = mock(Job.class);
        Mockito.doReturn(conf).when(mockJob).getConfiguration();
        return mockJob;
    }

    private void mockClientFactoryStatic(String scope, String uri) {
        PowerMockito.mockStatic(ClientFactory.class);
        ClientFactory mockClientFactory = mockClientFactory();
        PowerMockito.when(ClientFactory.withScope(scope, URI.create(uri))).thenReturn(mockClientFactory);
    }

    private ClientFactory mockClientFactory() {
        ClientFactory mockClientFactory = mock(ClientFactory.class);
        BatchClient mockBatchClient = mockBatchClient();
        Mockito.doReturn(mockBatchClient).when(mockClientFactory).createBatchClient();
        return mockClientFactory;
    }

    private BatchClient mockBatchClient() {
        BatchClient mockBatchClient = mock(BatchClient.class);
        Iterator<SegmentInfo> mockIterator = mockIterator();
        Mockito.doReturn(mockIterator).when(mockBatchClient).listSegments(anyObject());
        return mockBatchClient;
    }

    private Iterator<SegmentInfo> mockIterator() {
        Iterator<SegmentInfo> mockIterator = mock(Iterator.class);
        Mockito.when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        Mockito.when(mockIterator.next()).thenReturn(
                new SegmentInfo(new Segment(TEST_SCOPE, TEST_STREAM, 1), 0, 100, false, System.currentTimeMillis()),
                new SegmentInfo(new Segment(TEST_SCOPE, TEST_STREAM, 2), 0, 200, false, System.currentTimeMillis()),
                new SegmentInfo(new Segment(TEST_SCOPE, TEST_STREAM, 3), 0, 300, false, System.currentTimeMillis()));
        return mockIterator;
    }
}
