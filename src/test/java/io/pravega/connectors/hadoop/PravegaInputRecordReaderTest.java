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
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.impl.SegmentIteratorImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
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

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;


@RunWith(PowerMockRunner.class)
@PrepareForTest(ClientFactory.class)
public class PravegaInputRecordReaderTest {

    private static final String TEST_SCOPE = "PravegaInputRecordReaderTest";
    private static final String TEST_STREAM = "stream";
    private static final String TEST_URI = "tcp://127.0.0.1:9090";
    private static final Long END_OFFSET = 100L;

    private PravegaInputRecordReader<Integer> reader;
    private PravegaInputSplit split;
    private ClientFactory mockClientFactory;
    private SegmentIterator<Integer> mockIterator;
    private BatchClient mockBatchClient;

    @Before
    public void setupTests() throws Exception {
        mockClientFactoryStatic(TEST_SCOPE, TEST_URI);
        mockTaskAttemptContextImpl();
        reader = new PravegaInputRecordReader();
        split = new PravegaInputSplit(new Segment(TEST_SCOPE, TEST_STREAM, 10), 0, END_OFFSET);
        TaskAttemptContext ctx = new TaskAttemptContextImpl(null, null);
        reader.initialize(split, ctx);
        verify(ctx).getConfiguration();
        verify(mockBatchClient).readSegment(anyObject(), anyObject(), anyLong(), anyLong());
    }

    @Test
    public void testKeyValue() throws IOException, InterruptedException {
        int i = 0;
        while (reader.nextKeyValue()) {
            Assert.assertTrue(0 == reader.getCurrentKey().compareTo(new EventKey(split, 4 * i)));
            Assert.assertTrue(reader.getCurrentValue() == 10 * (i + 1));
            Assert.assertTrue(reader.getProgress() == ((float) (4 * i - 0)) / END_OFFSET);
            i++;
        }
        verify(mockIterator, times(4)).hasNext();
        verify(mockIterator, times(3)).next();
        verify(mockIterator, times(3)).getOffset();
    }

    @Test
    public void testClose() throws IOException {
        reader.close();
        verify(mockIterator).close();
        verify(mockClientFactory).close();
    }

    private void mockTaskAttemptContextImpl() throws Exception {
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, TEST_SCOPE);
        conf.setStrings(PravegaInputFormat.STREAM_NAME, TEST_STREAM);
        conf.setStrings(PravegaInputFormat.URI_STRING, TEST_URI);
        conf.setStrings(PravegaInputFormat.DESERIALIZER, IntegerSerializer.class.getName());

        TaskAttemptContextImpl mockTaskAttemptContextImpl = mock(TaskAttemptContextImpl.class);
        PowerMockito.whenNew(TaskAttemptContextImpl.class).withArguments(any(Configuration.class), any(TaskAttemptID.class)).thenReturn(mockTaskAttemptContextImpl);
        Mockito.doReturn(conf).when(mockTaskAttemptContextImpl).getConfiguration();
    }

    private void mockClientFactoryStatic(String scope, String uri) {
        PowerMockito.mockStatic(ClientFactory.class);
        mockClientFactory = mockClientFactory();
        PowerMockito.when(ClientFactory.withScope(scope, URI.create(uri))).thenReturn(mockClientFactory);
    }

    private ClientFactory mockClientFactory() {
        ClientFactory mockClientFactory = mock(ClientFactory.class);
        mockBatchClient = mockBatchClient();
        Mockito.doReturn(mockBatchClient).when(mockClientFactory).createBatchClient();
        return mockClientFactory;
    }

    private BatchClient mockBatchClient() {
        BatchClient mockBatchClient = mock(BatchClient.class);
        mockIterator = mockIterator();
        Mockito.doReturn(mockIterator).when(mockBatchClient).readSegment(anyObject(), anyObject(), anyLong(), anyLong());
        return mockBatchClient;
    }

    private SegmentIterator<Integer> mockIterator() {
        SegmentIterator<Integer> mockIterator = mock(SegmentIteratorImpl.class);
        Mockito.when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        Mockito.when(mockIterator.next()).thenReturn(new Integer(10), new Integer(20), new Integer(30));
        Mockito.when(mockIterator.getOffset()).thenReturn(0L, 4L, 8L);
        return mockIterator;
    }
}
