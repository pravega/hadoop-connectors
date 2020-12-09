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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Optional;
import java.util.Random;

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

    private static final Logger log = LoggerFactory.getLogger(PravegaOutputFormat.class);

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return getOutputRecordWriter(context);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final String tempDir = System.getProperty("java.io.tmpdir");
        final String randomId = String.valueOf(new Random(System.nanoTime()).nextInt());
        final String jobId = taskAttemptContext.getTaskAttemptID().getJobID().toString();
        StringBuilder path = new StringBuilder();
        path.append(tempDir).append(File.separator).append(jobId).append("-").append(randomId);
        return new FileOutputCommitter(new Path(path.toString()), taskAttemptContext);
    }

    private Object getInstanceFromName(String className) throws IOException {
        Object object = null;
        if (className != null) {
            try {
                Class<?> serializerClass = Class.forName(className);
                object = serializerClass.getDeclaredConstructor().newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                String errorMessage = "Unable to create instance for the class (" + className + ")";
                log.error(errorMessage, e);
                throw new IOException(errorMessage, e);
            }
        }
        return object;
    }

    private EventStreamWriter<V> getPravegaWriter(String scopeName,
                                                  String streamName,
                                                  ClientConfig clientConfig,
                                                  String serializerClassName) throws IOException {
        EventStreamClientFactory clientFactory = getClientFactory(scopeName, clientConfig);
        Object serializerInstance = getInstanceFromName(serializerClassName);
        if (!Serializer.class.isAssignableFrom(serializerInstance.getClass())) {
            throw new IOException(serializerInstance.getClass() + " is not a type of Serializer");
        }
        @SuppressWarnings("unchecked")
        Serializer<V> serializer = (Serializer<V>) serializerInstance;
        return clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());
    }

    @VisibleForTesting
    protected EventStreamClientFactory getClientFactory(String scope, ClientConfig clientConfig) {
        return EventStreamClientFactory.withScope(scope, clientConfig);
    }

    private PravegaOutputRecordWriter<V> getOutputRecordWriter(TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        final String streamName = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_STREAM_NAME)).orElseThrow(() ->
                new IOException("The output stream name must be configured (" + PravegaConfig.OUTPUT_STREAM_NAME + ")"));

        String scope;
        String stream;
        String[] scopedStream = streamName.split("/");
        if (scopedStream.length == 2) {
            scope = scopedStream[0];
            stream = scopedStream[1];
        } else {
            // check if scope is supplied separately?
            scope = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_SCOPE_NAME))
                    .orElseThrow(() -> new IOException("The output scope name must be configured (" + PravegaConfig.OUTPUT_SCOPE_NAME + ")"));
            stream = streamName;
        }

        final URI controllerURI = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaConfig.OUTPUT_URI_STRING + ")"));

        final String serializerClassName = Optional.ofNullable(conf.get(PravegaConfig.OUTPUT_SERIALIZER)).orElseThrow(() ->
                new IOException("The event serializer must be configured (" + PravegaConfig.OUTPUT_SERIALIZER + ")"));

        Object router = getInstanceFromName(conf.get(PravegaConfig.OUTPUT_EVENT_ROUTER));
        if (router != null && !PravegaEventRouter.class.isAssignableFrom(router.getClass())) {
            throw new IOException(router.getClass() + " is not a type of PravegaEventRouter");
        }

        ClientConfig clientConfig = SecurityHelper.prepareClientConfig(conf, controllerURI);

        @SuppressWarnings("unchecked")
        PravegaEventRouter<V> pravegaEventRouter = (PravegaEventRouter<V>) router;

        EventStreamWriter<V> eventStreamWriter = getPravegaWriter(scope, stream, clientConfig, serializerClassName);

        return new PravegaOutputRecordWriter<>(eventStreamWriter, pravegaEventRouter);
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
