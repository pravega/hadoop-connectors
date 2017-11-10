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

import io.pravega.client.ClientFactory;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * An InputFormat that can be added as a source to read from Pravega in a hadoop batch job.
 */
public class PravegaInputFormat<V> extends InputFormat<MetadataWritable, V> {

    // Pravega scope name
    public static final String SCOPE_NAME = "pravega.scope";
    // Pravega stream name
    public static final String STREAM_NAME = "pravega.stream";
    // Pravega uri string
    public static final String URI_STRING = "pravega.uri";
    // Pravega optional deserializer class name
    public static final String DESERIALIZER = "pravega.deserializer";

    /**
     * Generates splits which can be used by hadoop mapper to read pravega segments in parallel.
     * <p>
     * <p>The number of created input splits is equivalent to the parallelism of the source. For each input split,
     * a Pravega batch client will be created to read from the specified Pravega segment. Each input split is closed when
     * the nextKeyValue() returns false.
     *
     * @param context JobContext
     * @return List of InputSplits
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        List<InputSplit> splits = new ArrayList<InputSplit>();

        final String scopeName = conf.getRaw(PravegaInputFormat.SCOPE_NAME);
        final String streamName = conf.getRaw(PravegaInputFormat.STREAM_NAME);
        final URI controllerURI = URI.create(conf.getRaw(PravegaInputFormat.URI_STRING));
        try (ClientFactory clientFactory = ClientFactory.withScope(scopeName, controllerURI)) {
            BatchClient batchClient = clientFactory.createBatchClient();

            for (Iterator<SegmentInfo> iter = batchClient.listSegments(new StreamImpl(scopeName, streamName)); iter.hasNext(); ) {
                SegmentInfo segInfo = iter.next();
                Segment segment = segInfo.getSegment();
                PravegaInputSplit split = new PravegaInputSplit(segment, 0, segInfo.getLength());
                splits.add(split);
            }
        }
        return splits;
    }

    @Override
    public RecordReader<MetadataWritable, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        return new PravegaInputRecordReader<V>();
    }
}
