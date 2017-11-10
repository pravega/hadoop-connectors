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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class MetadataWritableTest {

    private static final String scope = "scope";
    private static final String stream = "stream";
    private Segment segment;
    private PravegaInputSplit split;
    private MetadataWritable key;

    @Before
    public void setUp() {
        segment = new Segment(scope, stream, 10);
        split = new PravegaInputSplit(segment, 1, 100);
        key = new MetadataWritable(split, 5L);
    }

    @Test
    public void testMetadataWritable() {
        Assert.assertEquals(5L, key.getOffset());
        Assert.assertEquals(split, key.getSplit());
        Assert.assertEquals(Long.valueOf(0), key.getTimestamp());
    }

    @Test
    public void testMetadataWritableWritable() throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOutput);
        key.write(out);
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOutput.toByteArray()));
        MetadataWritable inKey = new MetadataWritable();
        inKey.readFields(in);
        byteOutput.close();

        Assert.assertEquals(0, key.getSplit().getSegment().compareTo(inKey.getSplit().getSegment()));
        Assert.assertEquals(key.getOffset(), inKey.getOffset());
        Assert.assertEquals(key.getTimestamp(), inKey.getTimestamp());
    }
}
