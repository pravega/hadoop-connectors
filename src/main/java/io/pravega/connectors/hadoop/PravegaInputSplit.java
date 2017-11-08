/*
 * Copyright 2017 Dell/EMC
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.hadoop;

import io.pravega.client.segment.impl.Segment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hadoop InputSplit implementation for pravega storage, each InputSplit is one segment
 */
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

    // implements Writable
    public void readFields(DataInput in) throws IOException {
        segment = Segment.fromScopedName(Text.readString(in));
        startOffset = in.readLong();
        endOffset = in.readLong();
    }

    // implements Writable
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
