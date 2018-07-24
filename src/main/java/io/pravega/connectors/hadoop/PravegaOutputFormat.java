/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.hadoop;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OutputFormat that can be added as a storage to write events to Pravega.
 */
public class PravegaOutputFormat<V> extends OutputFormat<NullWritable, V> {

    // TODO: make it configurable when implementing build stype api (Issue 40)
    private static final long DEFAULT_TXN_TIMEOUT_MS = 30000L;

    private static final Logger log = LoggerFactory.getLogger(PravegaOutputFormat.class);

    // client factory
    private ClientFactory externalClientFactory;

    public PravegaOutputFormat() {
    }

    @VisibleForTesting
    protected PravegaOutputFormat(ClientFactory externalClientFactory) {
        this.externalClientFactory = externalClientFactory;
    }

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        final String scopeName = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_SCOPE_NAME)).orElseThrow(() ->
                new IOException("The input scope name must be configured (" + PravegaConfig.OUTPUT_SCOPE_NAME + ")"));
        final String streamName = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_STREAM_NAME)).orElseThrow(() ->
                new IOException("The input stream name must be configured (" + PravegaConfig.OUTPUT_STREAM_NAME + ")"));
        final URI controllerURI = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaConfig.OUTPUT_URI_STRING + ")"));
        final String serializerClassName = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_SERIALIZER)).orElseThrow(() ->
                new IOException("The event serializer must be configured (" + PravegaConfig.OUTPUT_SERIALIZER + ")"));

        return new PravegaOutputRecordWriter<>(getPravegaWriter(scopeName, streamName, controllerURI, serializerClassName));
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new PravegaOutputCommitter(new Path("/tmp/" + taskAttemptContext.getTaskAttemptID().getJobID().toString()), taskAttemptContext);
    }

    private EventStreamWriter<V> getPravegaWriter(
            String scopeName,
            String streamName,
            URI controllerURI,
            String serializerClassName) throws IOException {
        ClientFactory clientFactory = (externalClientFactory != null) ? externalClientFactory : ClientFactory.withScope(scopeName, controllerURI);

        Serializer serializer;
        try {
            Class<?> serializerClass = Class.forName(serializerClassName);
            serializer = (Serializer<V>) serializerClass.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            log.error("Exception when creating serializer: {}", e);
            throw new IOException(
                    "Unable to create the event serializer (" + serializerClassName + ")", e);
        }

        EventStreamWriter<V> writer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder()
                .transactionTimeoutTime(DEFAULT_TXN_TIMEOUT_MS)
                .build());

        return writer;
    }

    class PravegaOutputCommitter extends FileOutputCommitter {
        PravegaOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
            super(outputPath, context);
        }

        public void setupJob(JobContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            final String scopeName = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_SCOPE_NAME)).orElseThrow(() ->
                new IOException("The input scope name must be configured (" + PravegaConfig.OUTPUT_SCOPE_NAME + ")"));
            final String streamName = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_STREAM_NAME)).orElseThrow(() ->
                new IOException("The input stream name must be configured (" + PravegaConfig.OUTPUT_STREAM_NAME + ")"));
            final URI controllerURI = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaConfig.OUTPUT_URI_STRING + ")"));

            int inputScaling = Integer.parseInt(conf.get(PravegaConfig.OUTPUT_SCALING, "0"));
            final int scaling = inputScaling > 0 ? inputScaling : 1;

            createStream(scopeName, streamName, controllerURI, scaling);
            super.setupJob(context);
        }

        private void createStream(String scopeName, String streamName, URI controllerURI, int scaling) {
            StreamManager streamManager = StreamManager.create(controllerURI);
            streamManager.createScope(scopeName);

            StreamConfiguration streamConfig = StreamConfiguration.builder().scope(scopeName).streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(scaling))
                .build();

            streamManager.createStream(scopeName, streamName, streamConfig);
        }
    }

    /**
     * Gets a builder {@link PravegaOutputFormat} to write events to Pravega stream.
     * @return {@link PravegaOutputFormatBuilder}
     */
    public static PravegaOutputFormatBuilder builder() {
        return new PravegaOutputFormatBuilder();
    }

    /**
     * Gets a builder {@link PravegaOutputFormat} to write events to Pravega stream.
     *
     * @param conf Configuration
     * @return {@link PravegaOutputFormatBuilder}
     */
    public static PravegaOutputFormatBuilder builder(Configuration conf) {
        return new PravegaOutputFormatBuilder(conf);
    }
}
