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
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.ClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.connectors.hadoop.utils.IntegerSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;

public class PravegaInputFormatTest {

    private static final String TEST_SCOPE = "PravegaInputFormatTest";
    private static final String TEST_STREAM = "stream";
    private static final String TEST_URI = "tcp://127.0.0.1:9090";

    @Test
    public void testGetSplits() throws Exception {
        JobContext ctx = getJobContext();
        Configuration c = ctx.getConfiguration();
        Assert.assertNotNull(c);
        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat(mockClientFactory());
        List<InputSplit> splits = inputFormat.getSplits(ctx);
        Assert.assertEquals(splits.size(), 3);
        int i = 0;
        for (InputSplit s : splits) {
            Assert.assertTrue(s instanceof PravegaInputSplit);
            PravegaInputSplit ps = (PravegaInputSplit) s;
            i++;
            Segment segment = new Segment(TEST_SCOPE, TEST_STREAM, i);
            SegmentRange segmentRange = SegmentRangeImpl.builder().
                segment(segment).startOffset(0).endOffset(100 * i).build();
            Assert.assertTrue(0 == ps.compareTo(new PravegaInputSplit(segmentRange)));
        }
    }

    @Test
    public void testCreateRecordReader() throws IOException, InterruptedException {
        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat();
        RecordReader<?, ?> reader = inputFormat.createRecordReader(null, null);
        Assert.assertTrue(reader instanceof PravegaInputRecordReader);
    }

    @Test
    public void testFetchPositionsJson() throws IOException {
        Map<Segment, Long> origPositions = genPositions(10);
        ClientFactory clientFactory = mockClientFactory();
        String json = PravegaInputFormat.fetchPositionsJson(clientFactory, TEST_SCOPE, TEST_STREAM);
        Assert.assertFalse(json.isEmpty());

        Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization().create();

        Type type = new TypeToken<Map<Segment, Long>>() { }.getType();
        Map<Segment, Long> parsedPositions = gson.fromJson(json, type);

        Assert.assertEquals(origPositions.size(), parsedPositions.size());
        for (Map.Entry<Segment, Long> entry : parsedPositions.entrySet()) {
            Segment key = entry.getKey();
            Long value = entry.getValue();
            Assert.assertTrue(origPositions.containsKey(key));
            Assert.assertEquals(value, origPositions.get(key));
        }
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

    private ClientFactory mockClientFactory() {
        ClientFactory mockClientFactory = mock(ClientFactory.class);
        BatchClient mockBatchClient = mockBatchClient();
        Mockito.doReturn(mockBatchClient).when(mockClientFactory).createBatchClient();
        return mockClientFactory;
    }

    private BatchClient mockBatchClient() {
        BatchClient mockBatchClient = mock(BatchClient.class);
        StreamSegmentsIterator mockStreamSegmentsIterator = mockStreamSegmentsIterator();
        Mockito.doReturn(mockStreamSegmentsIterator).when(mockBatchClient)
            .getSegments(anyObject(), anyObject(), anyObject());
        return mockBatchClient;
    }

    private StreamSegmentsIterator mockStreamSegmentsIterator() {
        StreamSegmentsIterator mockStreamSegmentsIterator = mock(StreamSegmentsIterator.class);
        Mockito.doReturn(mockIterator()).when(mockStreamSegmentsIterator).getIterator();
        Stream s = new StreamImpl(TEST_SCOPE, TEST_STREAM);
        StreamCutImpl sc = new StreamCutImpl(s, genPositions(10));
        Mockito.doReturn(sc).when(mockStreamSegmentsIterator).getEndStreamCut();
        return mockStreamSegmentsIterator;
    }

    private Iterator<SegmentRange> mockIterator() {
        Iterator<SegmentRange> mockIterator = mock(Iterator.class);
        Mockito.when(mockIterator.hasNext()).thenReturn(true, true, true, false);
        Mockito.when(mockIterator.next()).thenReturn(
                SegmentRangeImpl.builder().segment(new Segment(TEST_SCOPE, TEST_STREAM, 1)).startOffset(0).endOffset(100).build(),
                SegmentRangeImpl.builder().segment(new Segment(TEST_SCOPE, TEST_STREAM, 2)).startOffset(0).endOffset(200).build(),
                SegmentRangeImpl.builder().segment(new Segment(TEST_SCOPE, TEST_STREAM, 3)).startOffset(0).endOffset(300).build());
        return mockIterator;
    }

    private Map<Segment, Long> genPositions(int n) {
        Map<Segment, Long> positions = new HashMap<>();
        for (int i = 0; i < n; i++) {
            positions.put(new Segment(TEST_SCOPE, TEST_STREAM, i), 10L * (i+1));
        }
        return positions;
    }

}
