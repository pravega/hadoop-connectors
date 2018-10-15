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

import io.pravega.client.stream.EventStreamWriter;
import org.apache.hadoop.io.NullWritable;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;


public class PravegaOutputRecordWriterTest {

    @Test
    public void testWriteAndClose() throws Exception {

        EventStreamWriter writer = mockEventStreamWriter();
        List<CompletableFuture<Void>> futures = mockWrite(writer);

        PravegaOutputRecordWriter<Integer> outputWriter = new PravegaOutputRecordWriter<>(writer, null);
        outputWriter.write(NullWritable.get(), 10);
        Assert.assertEquals(1, outputWriter.pendingWritesCount.get());
        outputWriter.write(NullWritable.get(), 20);
        Assert.assertEquals(2, outputWriter.pendingWritesCount.get());
        outputWriter.write(NullWritable.get(), 30);
        Assert.assertEquals(3, outputWriter.pendingWritesCount.get());

        futures.get(0).complete(null);
        futures.get(1).complete(null);
        futures.get(2).complete(null);

        Thread.sleep(10);
        Assert.assertEquals(0, outputWriter.pendingWritesCount.get());

        outputWriter.close(null);
        verify(writer).flush();
        verify(writer).close();
    }

    private EventStreamWriter mockEventStreamWriter() {
        EventStreamWriter writer = mock(EventStreamWriter.class);
        Mockito.doNothing().when(writer).flush();
        Mockito.doNothing().when(writer).close();
        return writer;
    }

    private List<CompletableFuture<Void>> mockWrite(EventStreamWriter writer) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(new CompletableFuture<Void>());
        }
        Mockito.when(writer.writeEvent(anyString(), anyObject())).thenReturn(futures.get(0), futures.get(1), futures.get(2));
        Mockito.when(writer.writeEvent(anyObject())).thenReturn(futures.get(0), futures.get(1), futures.get(2));
        return futures;
    }

    private static class WriteRuntimeException extends RuntimeException {
    }
}
