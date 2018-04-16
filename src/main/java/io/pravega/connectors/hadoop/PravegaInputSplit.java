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

import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.segment.impl.Segment;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Hadoop InputSplit implementation for Pravega.
 */
@NotThreadSafe
public class PravegaInputSplit extends InputSplit implements WritableComparable<PravegaInputSplit> {

    // Pravega SegmentRange
    private SegmentRange segmentRange;

    // Needed for reflection instantiation: Writable interface
    public PravegaInputSplit() {
    }

    /**
     * Creates an InputSplit corresponding to a Pravega segment.
     *
     * @param segmentRange     The pravega SegmentRange
     */
    public PravegaInputSplit(SegmentRange segmentRange) {
        this.segmentRange = segmentRange;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        String scopeName = Text.readString(in);
        String streamName = Text.readString(in);
        int segmentNumber = in.readInt();
        long startOffset = in.readLong();
        long endOffset = in.readLong();
        this.segmentRange = SegmentRangeImpl.builder()
            .segment(new Segment(scopeName, streamName, segmentNumber))
            .startOffset(startOffset)
            .endOffset(endOffset).build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.segmentRange.getScope());
        Text.writeString(out, this.segmentRange.getStreamName());
        out.writeInt(this.segmentRange.getSegmentNumber());
        out.writeLong(this.segmentRange.getStartOffset());
        out.writeLong(this.segmentRange.getEndOffset());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(PravegaInputSplit o) {
        int res = this.getSegment().compareTo(o.getSegment());
        if (res == 0) {
            res = Long.compare(this.getStartOffset(), o.getStartOffset());
        }
        if (res == 0) {
            res = Long.compare(this.getEndOffset(), o.getEndOffset());
        }
        return res;
    }

    public Segment getSegment() {
        return new Segment(segmentRange.getScope(), segmentRange.getStreamName(), segmentRange.getSegmentNumber());
    }

    public SegmentRange getSegmentRange() {
        return segmentRange;
    }

    public long getStartOffset() {
        return segmentRange.getStartOffset();
    }

    public long getEndOffset() {
        return segmentRange.getEndOffset();
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return getEndOffset() - getStartOffset();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{};
    }

    @Override
    public String toString() {
        return segmentRange.toString();
    }
}
