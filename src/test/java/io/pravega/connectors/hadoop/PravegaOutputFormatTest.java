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

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PravegaOutputFormatTest {

    //private final PravegaOutputFormat pravegaOutputFormat = mock(PravegaOutputFormat.class);

    private final TaskAttemptContext taskAttemptContext = mock(TaskAttemptContext.class);

    private final Configuration configuration = mock(Configuration.class);

    @Test (expected = IOException.class)
    public void testMissingScope() throws Exception {
        PravegaOutputFormat pravegaOutputFormat = new PravegaOutputFormat();
        final String stream = "foo";
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(configuration.get(PravegaConfig.OUTPUT_STREAM_NAME)).thenReturn(stream);
        when(configuration.get(PravegaConfig.OUTPUT_SCOPE_NAME)).thenReturn(null);
        pravegaOutputFormat.getRecordWriter(taskAttemptContext);
    }

    @Test (expected = IOException.class)
    public void testMissingStream() throws Exception {
        PravegaOutputFormat pravegaOutputFormat = new PravegaOutputFormat();
        final String scope = "foo";
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(configuration.get(PravegaConfig.OUTPUT_STREAM_NAME)).thenReturn(null);
        when(configuration.get(PravegaConfig.OUTPUT_SCOPE_NAME)).thenReturn(scope);
        pravegaOutputFormat.getRecordWriter(taskAttemptContext);
    }

    @Test (expected = IOException.class)
    public void testMissingControllerUri() throws Exception {
        PravegaOutputFormat pravegaOutputFormat = new PravegaOutputFormat();
        final String scope = "foo";
        final String stream = "bar";
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(configuration.get(PravegaConfig.OUTPUT_STREAM_NAME)).thenReturn(stream);
        when(configuration.get(PravegaConfig.OUTPUT_SCOPE_NAME)).thenReturn(scope);
        when(configuration.get(PravegaConfig.OUTPUT_URI_STRING)).thenReturn(null);
        pravegaOutputFormat.getRecordWriter(taskAttemptContext);
    }

    @Test (expected = IOException.class)
    public void testMissingSerializer() throws Exception {
        PravegaOutputFormat pravegaOutputFormat = new PravegaOutputFormat();
        final String scope = "foo";
        final String stream = "bar";
        final String controllerUri = "tcp://127.0.0.1:9090";
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(configuration.get(PravegaConfig.OUTPUT_SCOPE_NAME)).thenReturn(scope);
        when(configuration.get(PravegaConfig.OUTPUT_STREAM_NAME)).thenReturn(stream);
        when(configuration.get(PravegaConfig.OUTPUT_URI_STRING)).thenReturn(controllerUri);
        when(configuration.get(PravegaConfig.OUTPUT_SERIALIZER)).thenReturn(null);
        pravegaOutputFormat.getRecordWriter(taskAttemptContext);
    }

    @Test (expected = IOException.class)
    public void testInvalidEventRouter() throws Exception {
        PravegaOutputFormat<String> pravegaOutputFormat = new PravegaOutputFormat<>();
        final String scope = "foo";
        final String stream = "bar";
        final String controllerUri = "tcp://127.0.0.1:9090";
        final String serializer = JavaSerializer.class.getName();
        final String router = JavaSerializer.class.getName();
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(configuration.get(PravegaConfig.OUTPUT_SCOPE_NAME)).thenReturn(scope);
        when(configuration.get(PravegaConfig.OUTPUT_STREAM_NAME)).thenReturn(stream);
        when(configuration.get(PravegaConfig.OUTPUT_URI_STRING)).thenReturn(controllerUri);
        when(configuration.get(PravegaConfig.OUTPUT_SERIALIZER)).thenReturn(serializer);
        when(configuration.get(PravegaConfig.OUTPUT_EVENT_ROUTER)).thenReturn(router);
        PravegaOutputFormat<String> spyPravegaOutputFormat = spy(pravegaOutputFormat);
        mockClientFactory(spyPravegaOutputFormat);
        spyPravegaOutputFormat.getRecordWriter(taskAttemptContext);
    }

    @Test
    public void testGetRecordWriter() throws Exception {
        PravegaOutputFormat<String> pravegaOutputFormat = new PravegaOutputFormat<>();
        final String scope = "foo";
        final String stream = "bar";
        final String controllerUri = "tcp://127.0.0.1:9090";
        final String serializer = JavaSerializer.class.getName();
        final String router = EventRouter.class.getName();
        when(taskAttemptContext.getConfiguration()).thenReturn(configuration);
        when(configuration.get(PravegaConfig.OUTPUT_SCOPE_NAME)).thenReturn(scope);
        when(configuration.get(PravegaConfig.OUTPUT_STREAM_NAME)).thenReturn(stream);
        when(configuration.get(PravegaConfig.OUTPUT_URI_STRING)).thenReturn(controllerUri);
        when(configuration.get(PravegaConfig.OUTPUT_SERIALIZER)).thenReturn(serializer);
        when(configuration.get(PravegaConfig.OUTPUT_EVENT_ROUTER)).thenReturn(router);
        PravegaOutputFormat<String> spyPravegaOutputFormat = spy(pravegaOutputFormat);
        mockClientFactory(spyPravegaOutputFormat);
        spyPravegaOutputFormat.getRecordWriter(taskAttemptContext);
        verify(spyPravegaOutputFormat).getRecordWriter(taskAttemptContext);
    }

    @SuppressWarnings("unchecked")
    private <T> EventStreamWriter<T> mockEventStreamWriter() {
        return mock(EventStreamWriter.class);
    }

    private void mockClientFactory(PravegaOutputFormat<String> spyPravegaOutputFormat) {
        EventStreamClientFactory mockClientFactory = mock(EventStreamClientFactory.class);
        doReturn(mockClientFactory).when(spyPravegaOutputFormat).getClientFactory(anyString(), anyObject());
        doReturn(mockEventStreamWriter())
                .when(mockClientFactory)
                .createEventWriter(anyString(), any(Serializer.class), any(EventWriterConfig.class));
    }

    static class EventRouter implements PravegaEventRouter<String> {
        @Override
        public String getRoutingKey(String event) {
            return "fixedKey";
        }
    }
}
