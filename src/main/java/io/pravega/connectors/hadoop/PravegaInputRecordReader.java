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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.stream.Serializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A RecordReader that can read events from an InputSplit as provided by {@link PravegaInputFormat}.
 */
@NotThreadSafe
public class PravegaInputRecordReader<V> extends RecordReader<EventKey, V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaInputRecordReader.class);
    private BatchClientFactory clientFactory;
    private BatchClientFactory externalClientFactory;
    private PravegaInputSplit split;
    private SegmentIterator<V> iterator;
    // Pravega Serializer to deserialize events saved in pravega
    private Serializer<V> deserializer;

    private EventKey key;
    private V value;

    public PravegaInputRecordReader() {
    }

    @VisibleForTesting
    protected PravegaInputRecordReader(BatchClientFactory externalClientFactory) {
        this.externalClientFactory = externalClientFactory;
    }

    /**
     * Initializes RecordReader by InputSplit and TaskAttemptContext.
     *
     * <p>Connects to Pravega and prepares to read events in the InputSplit.
     *
     * @param split   InputSplit
     * @param context TaskAttemptContext
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        this.split = (PravegaInputSplit) split;
        Configuration conf = context.getConfiguration();

        final URI controllerURI = Optional.ofNullable(conf.get(PravegaConfig.INPUT_URI_STRING)).map(URI::create).orElseThrow(() ->
                new IOException("The Pravega controller URI must be configured (" + PravegaConfig.INPUT_URI_STRING + ")"));

        ClientConfig clientConfig = SecurityHelper.prepareClientConfig(conf, controllerURI);

        clientFactory = (externalClientFactory != null) ? externalClientFactory : BatchClientFactory.withScope(conf.get(PravegaConfig.INPUT_SCOPE_NAME), clientConfig);

        String deserializerClassName = conf.get(PravegaConfig.INPUT_DESERIALIZER);
        try {
            Class<?> deserializerClass = Class.forName(deserializerClassName);
            deserializer = (Serializer<V>) deserializerClass.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            log.error("Exception when creating deserializer: {}", e);
            throw new IOException("Unable to create the event deserializer (" + deserializerClassName + ")", e);
        }
        iterator = clientFactory.readSegment(this.split.getSegmentRange(), deserializer);
    }

    /**
     * Retrieves the next key/value pair from the InputSplit.
     *
     * @return next key/value exists or not
     */
    @Override
    public synchronized boolean nextKeyValue() throws IOException, InterruptedException {
        if (iterator.hasNext()) {
            key = new EventKey(split.getSegment(), iterator.getOffset());
            value = iterator.next();
            log.debug("Key: {}, Value: {} ({})", key, value, value.getClass().getName());
            return true;
        }
        return false;
    }

    /**
     * Gets the key associated with the current key/value pair.
     */
    @Override
    public EventKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * Gets the value associated with the current key/value pair.
     */
    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public synchronized float getProgress() throws IOException, InterruptedException {
        if (key != null && split.getLength() > 0) {
            return ((float) (key.getOffset() - split.getStartOffset())) / split.getLength();
        }
        return 0.0f;
    }

    @Override
    public synchronized void close() throws IOException {
        if (iterator != null) {
            iterator.close();
        }
        // clientFactory is created by myself, so close it
        if (clientFactory != null && externalClientFactory == null) {
            clientFactory.close();
        }
    }
}
