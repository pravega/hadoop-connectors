/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.hadoop.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.local.InProcPravegaCluster;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.UUID;

/**
 * Utility functions for creating the test setup.
 */
@Slf4j
@NotThreadSafe
public final class SetupUtils {
    // Manage the state of the class.
    private final AtomicBoolean started = new AtomicBoolean(false);
    // The pravega cluster.
    private InProcPravegaCluster inProcPravegaCluster = null;
    // The test Scope name.
    @Getter
    private String scope;
    // The controller port.
    @Getter
    private int controllerPort;

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @param scope the scope to use.
     * @throws Exception on any errors.
     */
    public void startAllServices(String scope) throws Exception {
        this.scope = scope;
        if (!this.started.compareAndSet(false, true)) {
            log.warn("Services already started, not attempting to start again");
            return;
        }

        int zkPort = TestUtils.getAvailableListenPort();
        controllerPort = TestUtils.getAvailableListenPort();
        int hostPort = TestUtils.getAvailableListenPort();
        int restPort = TestUtils.getAvailableListenPort();

        this.inProcPravegaCluster = InProcPravegaCluster.builder()
                .isInProcZK(true)
                .zkUrl("localhost:" + zkPort)
                .zkPort(zkPort)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .restServerPort(restPort)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .enableTls(false)
                // to match code change https://github.com/pravega/pravega/pull/2476
                .keyFile("")
                .certFile("")
                .enableAuth(false)
                .userName("")
                .passwd("")
                .passwdFile("")
                .jksKeyFile("")
                .jksTrustFile("")
                .keyPasswordFile("")
                .build();
        this.inProcPravegaCluster.setControllerPorts(new int[]{controllerPort});
        this.inProcPravegaCluster.setSegmentStorePorts(new int[]{hostPort});
        this.inProcPravegaCluster.start();
        log.info("Initialized Pravega Cluster");
        log.info("Controller port is {}", controllerPort);
        log.info("Host port is {}", hostPort);
        log.info("REST server port is {}", restPort);
    }

    /**
     * Stop the pravega cluster and release all resources.
     *
     * @throws Exception on any errors.
     */
    public void stopAllServices() throws Exception {
        if (!this.started.compareAndSet(true, false)) {
            log.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        this.inProcPravegaCluster.close();
    }

    /**
     * Fetch the controller endpoint for this cluster.
     *
     * @return The controller endpoint to connect to this cluster.
     */
    public String getControllerUri() {
        return this.inProcPravegaCluster.getControllerURI();
    }

    /**
     * Create the test stream.
     *
     * @param streamName  Name of the test stream.
     * @param numSegments Number of segments to be created for this stream.
     * @throws Exception on any errors.
     */
    public void createTestStream(final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        @Cleanup
        StreamManager streamManager = StreamManager.create(URI.create(getControllerUri()));
        streamManager.createScope(this.scope);
        streamManager.createStream(this.scope, streamName,
                StreamConfiguration.builder()
                        .scope(this.scope)
                        .streamName(streamName)
                        .scalingPolicy(ScalingPolicy.fixed(numSegments))
                        .build());
        log.info("Created stream: " + streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param streamName Name of the test stream.
     * @return Stream writer instance.
     */
    public EventStreamWriter<Integer> getIntegerWriter(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ClientFactory clientFactory = ClientFactory.withScope(this.scope, URI.create(getControllerUri()));
        return clientFactory.createEventWriter(
                streamName,
                new IntegerSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream writer for writing string events.
     *
     * @param streamName Name of the test stream.
     * @return Stream writer instance.
     */
    public EventStreamWriter<String> getStringWriter(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ClientFactory clientFactory = ClientFactory.withScope(this.scope, URI.create(getControllerUri()));
        return clientFactory.createEventWriter(
                streamName,
                new JavaSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading string events.
     *
     * @param streamName Name of the test stream.
     * @return Stream reader instance.
     */
    public EventStreamReader<String> getStringReader(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(this.scope, getClientConfig());
        final String readerGroup = "testReaderGroup" + this.scope + streamName;
        Stream stream = Stream.of(this.scope, streamName);
        readerGroupManager.createReaderGroup(
                readerGroup,
                //ReaderGroupConfig.builder().stream(stream, getStreamCut(streamName, 0L)).build());
                ReaderGroupConfig.builder().stream(stream).build());

        ClientFactory clientFactory = ClientFactory.withScope(this.scope, getClientConfig());
        final String readerGroupId = UUID.randomUUID().toString();
        return clientFactory.createReader(
                readerGroupId,
                readerGroup,
                new JavaSerializer<String>(),
                ReaderConfig.builder().build());
    }


    public ClientConfig getClientConfig() {
        return ClientConfig.builder()
                .controllerURI(URI.create(getControllerUri()))
                .credentials(new DefaultCredentials("", ""))
                .build();
    }

    private StreamCut getStreamCut(String streamName, long offset) {
        ImmutableMap<Segment, Long> positions = ImmutableMap.<Segment, Long>builder().put(new Segment(this.scope,
                streamName, 0), offset).build();
        return new StreamCutImpl(Stream.of(this.scope, streamName), positions);
    }
}
