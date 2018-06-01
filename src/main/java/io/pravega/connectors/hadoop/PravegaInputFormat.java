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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import io.pravega.client.ClientFactory;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * An InputFormat that can be added as a source to read from Pravega in a hadoop batch job.
 */
public class PravegaInputFormat<V> extends InputFormat<EventKey, V> {

    // Pravega scope name
    public static final String SCOPE_NAME = "pravega.scope";
    // Pravega stream name
    public static final String STREAM_NAME = "pravega.stream";
    // Pravega uri string
    public static final String URI_STRING = "pravega.uri";
    // Pravega deserializer class name
    public static final String DESERIALIZER = "pravega.deserializer";
    // Pravega optional start streamcut
    public static final String START_POSITIONS = "pravega.startpositions";
    // Pravega optional end streamcut
    public static final String END_POSITIONS = "pravega.endpositions";
    // client factory
    private ClientFactory externalClientFactory;

    public PravegaInputFormat() {
    }

    @VisibleForTesting
    protected PravegaInputFormat(ClientFactory externalClientFactory) {
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

        final String scopeName = Optional.ofNullable(conf.get(PravegaInputFormat.SCOPE_NAME)).orElseThrow(() ->
                new IOException("The input scope name must be configured (" + PravegaInputFormat.SCOPE_NAME + ")"));
        final String streamName = Optional.ofNullable(conf.get(PravegaInputFormat.STREAM_NAME)).orElseThrow(() ->
                new IOException("The input stream name must be configured (" + PravegaInputFormat.STREAM_NAME + ")"));
        final URI controllerURI = Optional.ofNullable(conf.get(PravegaInputFormat.URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaInputFormat.URI_STRING + ")"));
        final String deserializerClassName = Optional.ofNullable(conf.get(PravegaInputFormat.DESERIALIZER)).orElseThrow(() ->
                new IOException("The event deserializer must be configured (" + PravegaInputFormat.DESERIALIZER + ")"));

        final StreamCut startStreamCut = getStreamCutFromPositionsJson(
                scopeName,
                streamName,
                Optional.ofNullable(conf.get(PravegaInputFormat.START_POSITIONS)).orElse(""));
        final StreamCut endStreamCut = getStreamCutFromPositionsJson(
                scopeName,
                streamName,
                Optional.ofNullable(conf.get(PravegaInputFormat.END_POSITIONS)).orElse(""));

        ClientFactory clientFactory = (externalClientFactory != null) ? externalClientFactory : ClientFactory.withScope(scopeName, controllerURI);

        // generate a split per segment
        List<InputSplit> splits = new ArrayList<InputSplit>();
        BatchClient batchClient = clientFactory.createBatchClient();
        Iterator<SegmentRange> iter = batchClient.getSegments(Stream.of(scopeName, streamName), startStreamCut, endStreamCut).getIterator();
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

    public static String fetchLatestPositionsJson(String uri, String scopeName, String streamName) throws IOException {
        Preconditions.checkArgument(uri != null && !uri.isEmpty());
        Preconditions.checkArgument(scopeName != null && !scopeName.isEmpty());
        Preconditions.checkArgument(streamName != null && !streamName.isEmpty());

        String pos = "";
        try (ClientFactory clientFactory = ClientFactory.withScope(scopeName, URI.create(uri))) {
            pos = fetchPositionsJson(clientFactory, scopeName, streamName);
        }
        return pos;
    }

    @VisibleForTesting
    public static String fetchPositionsJson(ClientFactory clientFactory, String scopeName, String streamName) throws IOException {
        BatchClient batchClient = clientFactory.createBatchClient();
        StreamInfo streamInfo = batchClient.getStreamInfo(Stream.of(scopeName, streamName)).join();
        StreamCut endStreamCut = streamInfo.getTailStreamCut();

        Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization().create();
        return gson.toJson(endStreamCut.asImpl().getPositions());
    }

    private StreamCut getStreamCutFromPositionsJson(String scopeName, String streamName, String s) throws IOException {
        StreamCut sc = null;
        if (s == null || s.isEmpty()) {
            return sc;
        }

        Gson gson = new GsonBuilder()
            .enableComplexMapKeySerialization().create();

        Type type = new TypeToken<Map<Segment, Long>>() { }.getType();
        Map<Segment, Long> positions = gson.fromJson(s, type);

        sc = new StreamCutImpl(Stream.of(scopeName, streamName), positions);
        return sc;
    }
}
