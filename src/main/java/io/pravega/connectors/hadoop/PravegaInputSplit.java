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

import io.pravega.client.segment.impl.Segment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Hadoop InputSplit implementation for pravega storage, each InputSplit is one segment
 */
@NotThreadSafe
public class PravegaInputSplit extends InputSplit implements Writable {

    // Pravega segment
    private Segment segment;
    // start offset in the segment
    private long startOffset;
    // end offset in the segment
    private long endOffset;

    // Needed for reflection instantiation: Writable interface
    public PravegaInputSplit() {
    }

    /**
     * Creates an InputSplit to map pravega segment
     *
     * @param segment     The pravega segment
     * @param startOffset start offset in the segment
     * @param endOffset   end offset in the segment
     */
    public PravegaInputSplit(Segment segment, long startOffset, long endOffset) {
        this.segment = segment;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    /**
     * Implements Writable interface
     *
     * @param in DataInput
     */
    public void readFields(DataInput in) throws IOException {
        segment = Segment.fromScopedName(Text.readString(in));
        startOffset = in.readLong();
        endOffset = in.readLong();
    }

    /**
     * Implements Writable interface
     *
     * @param out DataOutput
     */
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, segment.getScopedName());
        out.writeLong(startOffset);
        out.writeLong(endOffset);
    }

    public Segment getSegment() {
        return segment;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return endOffset - startOffset;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    @Override
    public String toString() {
        return String.format("%s:%s:%s", segment.getScopedName(), String.valueOf(getStartOffset()), String.valueOf(getEndOffset()));
    }
}
