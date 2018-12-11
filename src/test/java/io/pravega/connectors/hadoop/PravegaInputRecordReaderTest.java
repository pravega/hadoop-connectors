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

import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.impl.SegmentIteratorImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.io.IOException;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;


public class PravegaInputRecordReaderTest {

    private static final String TEST_SCOPE = "PravegaInputRecordReaderTest";
    private static final String TEST_STREAM = "stream";
    private static final String TEST_URI = "tcp://127.0.0.1:9090";
    private static final Long END_OFFSET = 100L;

    private PravegaInputRecordReader<Integer> reader;
    private Segment segment;
    private PravegaInputSplit split;
    private BatchClientFactory mockClientFactory;
    private SegmentIterator<Integer> mockIterator;

    @Before
    public void setupTests() throws Exception {
        mockClientFactory = mockClientFactory();
        reader = new PravegaInputRecordReader(mockClientFactory);
        segment = new Segment(TEST_SCOPE, TEST_STREAM, 10);
        SegmentRange segmentRange = SegmentRangeImpl.builder().segment(segment)
            .startOffset(0).endOffset(END_OFFSET).build();
        split = new PravegaInputSplit(segmentRange);
        TaskAttemptContext ctx = mockTaskAttemptContextImpl();
        reader.initialize(split, ctx);
        verify(ctx).getConfiguration();
        verify(mockClientFactory).readSegment(anyObject(), anyObject());
    }

    @Test
    public void testKeyValue() throws IOException, InterruptedException {
        int i = 0;
        while (reader.nextKeyValue()) {
            Assert.assertTrue(0 == reader.getCurrentKey().compareTo(new EventKey(segment, 4 * i)));
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
    }

    private TaskAttemptContext mockTaskAttemptContextImpl() throws Exception {
        Configuration conf = PravegaInputFormat.builder()
            .withScope(TEST_SCOPE)
            .forStream(TEST_STREAM)
            .withURI(TEST_URI)
            .withDeserializer(IntegerSerializer.class.getName())
            .build();

        TaskAttemptContextImpl mockTaskAttemptContextImpl = mock(TaskAttemptContextImpl.class);
        Mockito.doReturn(conf).when(mockTaskAttemptContextImpl).getConfiguration();
        return mockTaskAttemptContextImpl;
    }

    private BatchClientFactory mockClientFactory() {
        BatchClientFactory mockClientFactory = mock(BatchClientFactory.class);
        mockIterator = mockIterator();
        Mockito.doReturn(mockIterator).when(mockClientFactory).readSegment(anyObject(), anyObject());
        return mockClientFactory;
    }

    private SegmentIterator<Integer> mockIterator() {
        SegmentIterator<Integer> mockIterator = mock(SegmentIteratorImpl.class);
        Mockito.when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        Mockito.when(mockIterator.next()).thenReturn(new Integer(10), new Integer(20), new Integer(30));
        Mockito.when(mockIterator.getOffset()).thenReturn(0L, 4L, 8L);
        return mockIterator;
    }
}
