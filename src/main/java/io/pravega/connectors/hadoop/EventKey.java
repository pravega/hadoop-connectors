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

import io.pravega.client.segment.impl.Segment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A writable key class for records (events) produced by {@link PravegaInputRecordReader}.
 */
@NotThreadSafe
public class EventKey implements WritableComparable<EventKey>, Serializable {

    private static final long serialVersionUID = 1L;
    private Segment segment;
    private long offset;

    /**
     * Constructor used by Hadoop to init the class through reflection. Do not remove.
     */
    public EventKey() {
    }

    public EventKey(Segment segment, long offset) {
        this.segment = segment;
        this.offset = offset;
    }

    /**
     * Gets the offset into the segment associated with the event.
     * @return offset in key
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Gets the segment associated with the event.
     * @return segment in key
     */
    public Segment getSegment() {
        return segment;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, segment.getScopedName());
        out.writeLong(getOffset());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        segment = Segment.fromScopedName(Text.readString(in));
        offset = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(EventKey o) {
        int res = segment.compareTo(o.getSegment());
        if (res == 0) {
            res = Long.compare(offset, o.getOffset());
        }
        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("%s:%s", segment.toString(), String.valueOf(getOffset()));
    }
}
