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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * An InputFormat that can be added as a source to read from Pravega in a hadoop batch job.
 */
public class PravegaInputFormat<V> extends InputFormat<EventKey, V> {

    // client factory
    private BatchClientFactory externalClientFactory;

    public PravegaInputFormat() {
    }

    @VisibleForTesting
    protected PravegaInputFormat(BatchClientFactory externalClientFactory) {
        this.externalClientFactory = externalClientFactory;
    }

    /**
     * Generates splits which can be used by hadoop mapper to read pravega segments in parallel.
     *
     * The number of created input splits is equivalent to the parallelism of the source. For each input split,
     * a Pravega batch client will be created to read from the specified Pravega segment. Each input split is closed when
     * the nextKeyValue() returns false.
     *
     * @param context JobContext
     * @return List of InputSplits
     */
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        // check parameters
        Configuration conf = context.getConfiguration();

        final String scopeName = Optional.ofNullable(conf.get(PravegaConfig.INPUT_SCOPE_NAME)).orElseThrow(() ->
                new IOException("The input scope name must be configured (" + PravegaConfig.INPUT_SCOPE_NAME + ")"));
        final String streamName = Optional.ofNullable(conf.get(PravegaConfig.INPUT_STREAM_NAME)).orElseThrow(() ->
                new IOException("The input stream name must be configured (" + PravegaConfig.INPUT_STREAM_NAME + ")"));
        final URI controllerURI = Optional.ofNullable(conf.get(PravegaConfig.INPUT_URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaConfig.INPUT_URI_STRING + ")"));
        final String deserializerClassName = Optional.ofNullable(conf.get(PravegaConfig.INPUT_DESERIALIZER)).orElseThrow(() ->
                new IOException("The event deserializer must be configured (" + PravegaConfig.INPUT_DESERIALIZER + ")"));

        final StreamCut startStreamCut = getStreamCutFromString(
                Optional.ofNullable(conf.get(PravegaConfig.INPUT_START_POSITION)).orElse(""));
        final StreamCut endStreamCut = getStreamCutFromString(
                Optional.ofNullable(conf.get(PravegaConfig.INPUT_END_POSITION)).orElse(""));

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerURI).build();
        BatchClientFactory clientFactory = (externalClientFactory != null) ? externalClientFactory : BatchClientFactory.withScope(scopeName, clientConfig);

        // generate a split per segment
        List<InputSplit> splits = new ArrayList<InputSplit>();
        Iterator<SegmentRange> iter = clientFactory.getSegments(Stream.of(scopeName, streamName), startStreamCut, endStreamCut).getIterator();
        while (iter.hasNext()) {
            SegmentRange sr = iter.next();
            PravegaInputSplit split = new PravegaInputSplit(sr);
            splits.add(split);
        }
        // close it only if it's created by myself
        if (externalClientFactory == null) {
            clientFactory.close();
        }
        return splits;
    }

    @Override
    public RecordReader<EventKey, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        return new PravegaInputRecordReader<V>();
    }

    public static String fetchLatestPosition(String uri, String scopeName, String streamName) throws IOException {
        Preconditions.checkArgument(uri != null && !uri.isEmpty());
        Preconditions.checkArgument(scopeName != null && !scopeName.isEmpty());
        Preconditions.checkArgument(streamName != null && !streamName.isEmpty());

        String pos = "";
        try (StreamManager streamManager = StreamManager.create(URI.create(uri))) {
            pos = fetchPosition(streamManager, scopeName, streamName);
        }
        return pos;
    }

    @VisibleForTesting
    public static String fetchPosition(StreamManager streamManager, String scopeName, String streamName) throws IOException {
        StreamInfo streamInfo = streamManager.getStreamInfo(scopeName, streamName);
        StreamCut endStreamCut = streamInfo.getTailStreamCut();
        return Base64.getEncoder().encodeToString(endStreamCut.toBytes().array());
    }

    private StreamCut getStreamCutFromString(String value) throws IOException {
        StreamCut sc = null;
        if (value == null || value.isEmpty()) {
            return sc;
        }

        ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(value));
        sc = StreamCut.fromBytes(buf);

        return sc;
    }

    /**
     * Gets a builder {@link PravegaInputFormat} to read Pravega streams using the batch API.
     * @return {@link PravegaInputFormatBuilder}
     */
    public static PravegaInputFormatBuilder builder() {
        return new PravegaInputFormatBuilder();
    }

    /**
     * Gets a builder {@link PravegaInputFormat} to read Pravega streams using the batch API.
     *
     * @param conf Configuration
     * @return {@link PravegaInputFormatBuilder}
     */
    public static PravegaInputFormatBuilder builder(Configuration conf) {
        return new PravegaInputFormatBuilder(conf);
    }
}
