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

import io.pravega.client.stream.EventStreamWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.UUID;

/**
 * A RecordWriter that can write events to Pravega.
 */
@NotThreadSafe
public class PravegaOutputRecordWriter<V> extends RecordWriter<NullWritable, V> {

    private static final Logger log = LoggerFactory.getLogger(PravegaOutputRecordWriter.class);

    protected final AtomicInteger pendingWritesCount;
    private final AtomicReference<Throwable> writeError;
    private final ExecutorService executorService;
    private final EventStreamWriter writer;

    public PravegaOutputRecordWriter(EventStreamWriter writer) {
        this.writer = writer;
        this.pendingWritesCount = new AtomicInteger(0);
        this.writeError = new AtomicReference<>(null);
        this.executorService = Executors.newFixedThreadPool(5);
    }

    @Override
    public void write(NullWritable ignored, V value) throws IOException, InterruptedException {

        checkWriteError();

        this.pendingWritesCount.incrementAndGet();
        // write to segments randomly because we don't care which segment to write
        final CompletableFuture<Void> future = writer.writeEvent(UUID.randomUUID().toString(), value);
        future.whenCompleteAsync(
            (v, e) -> {
                if (e != null) {
                    log.warn("Detected a write failure: {}", e);
                    writeError.compareAndSet(null, e);
                }
                synchronized (this) {
                    this.pendingWritesCount.decrementAndGet();
                    this.notify();
                }
            },
            executorService
        );
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            flushAndVerify();
        } catch (IOException | InterruptedException e) {
            throw e;
        } finally {
            this.writer.close();
            this.executorService.shutdown();
        }
    }

    private void checkWriteError() throws IOException {
        Throwable error = this.writeError.getAndSet(null);
        if (error != null) {
            throw new IOException("Write failure", error);
        }
    }

    private void flushAndVerify() throws IOException, InterruptedException {
        this.writer.flush();

        synchronized (this) {
            while (this.pendingWritesCount.get() > 0) {
                this.wait();
            }
        }

        checkWriteError();
    }
}
