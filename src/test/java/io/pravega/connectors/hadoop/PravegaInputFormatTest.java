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

import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
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
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
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
        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat<>(mockClientFactory());
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
        PravegaInputFormat<Integer> inputFormat = new PravegaInputFormat<>();
        RecordReader<?, ?> reader = inputFormat.createRecordReader(null, null);
        Assert.assertTrue(reader instanceof PravegaInputRecordReader);
    }

    @Test
    public void testFetchPosition() throws IOException {
        StreamCut sc = new StreamCutImpl(Stream.of(TEST_SCOPE, TEST_STREAM), genPositions(10));

        StreamManager streamManager = mockStreamManager();
        String value = PravegaInputFormat.fetchPosition(streamManager, TEST_SCOPE, TEST_STREAM);

        ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(value));
        Assert.assertEquals(sc, StreamCut.fromBytes(buf));
    }

    private JobContext getJobContext() throws Exception {
        Configuration conf = PravegaInputFormat.builder()
            .withScope(TEST_SCOPE)
            .forStream(TEST_STREAM)
            .withURI(TEST_URI)
            .withDeserializer(IntegerSerializer.class.getName())
            .build();
        Job mockJob = mock(Job.class);
        Mockito.doReturn(conf).when(mockJob).getConfiguration();
        return mockJob;
    }

    private BatchClientFactory mockClientFactory() {
        BatchClientFactory mockClientFactory = mock(BatchClientFactory.class);
        StreamSegmentsIterator mockStreamSegmentsIterator = mockStreamSegmentsIterator();
        Mockito.doReturn(mockStreamSegmentsIterator).when(mockClientFactory)
                .getSegments(anyObject(), anyObject(), anyObject());
        return mockClientFactory;
    }

    private StreamManager mockStreamManager() {
        StreamManager streamManager = mock(StreamManager.class);
        io.pravega.client.admin.StreamInfo streamInfo = mock(io.pravega.client.admin.StreamInfo.class);
        Mockito.doReturn(streamInfo).when(streamManager).getStreamInfo(anyString(), anyString());
        Stream s = new StreamImpl(TEST_SCOPE, TEST_STREAM);
        StreamCut sc = new StreamCutImpl(s, genPositions(10));
        Mockito.doReturn(sc).when(streamInfo).getTailStreamCut();
        return streamManager;
    }

    private StreamSegmentsIterator mockStreamSegmentsIterator() {
        StreamSegmentsIterator mockStreamSegmentsIterator = mock(StreamSegmentsIterator.class);
        Mockito.doReturn(mockIterator()).when(mockStreamSegmentsIterator).getIterator();
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

    @Test
    public void testConfigBuilder() {
        String[] params = new String[]{"scope", "stream", "tcp://localhost:9090", "class1", "1234", "5678"};
        int idx = 0;
        // generate config from nothing
        Configuration conf1 = PravegaInputFormat.builder()
            .withScope(params[idx++])
            .forStream(params[idx++])
            .withURI(params[idx++])
            .withDeserializer(params[idx++])
            .startPosition(params[idx++])
            .endPosition(params[idx++])
            .build();
        checkJobConf(conf1, params);

        // add properties to existing config
        Configuration conf2 = new Configuration();
        conf2.setStrings("OTHERS", "something");
        idx = 0;
        conf2 = PravegaInputFormat.builder(conf2)
            .withScope(params[idx++])
            .forStream(params[idx++])
            .withURI(params[idx++])
            .withDeserializer(params[idx++])
            .startPosition(params[idx++])
            .endPosition(params[idx++])
            .build();
        checkJobConf(conf2, params);
        Assert.assertEquals("something", conf2.get("OTHERS"));
    }

    private void checkJobConf(Configuration conf, String[] params) {
        Assert.assertEquals(params.length, 6);
        int idx = 0;
        Assert.assertEquals(conf.get(PravegaConfig.INPUT_SCOPE_NAME), params[idx++]);
        Assert.assertEquals(conf.get(PravegaConfig.INPUT_STREAM_NAME), params[idx++]);
        Assert.assertEquals(conf.get(PravegaConfig.INPUT_URI_STRING), params[idx++]);
        Assert.assertEquals(conf.get(PravegaConfig.INPUT_DESERIALIZER), params[idx++]);
        Assert.assertEquals(conf.get(PravegaConfig.INPUT_START_POSITION), params[idx++]);
        Assert.assertEquals(conf.get(PravegaConfig.INPUT_END_POSITION), params[idx++]);
    }
}
