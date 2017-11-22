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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class EventKeyTest {

    private static final String TEST_SCOPE = "EventKeyTest";
    private static final String TEST_STREAM = "stream";

    private Segment segment;
    private PravegaInputSplit split;
    private EventKey key;

    @Before
    public void setUp() {
        segment = new Segment(TEST_SCOPE, TEST_STREAM, 10);
        split = new PravegaInputSplit(segment, 1, 100);
        key = new EventKey(split, 5L);
    }

    @Test
    public void testEventKey() {
        Assert.assertEquals(5L, key.getOffset());
        Assert.assertEquals(split, key.getSplit());
    }

    @Test
    public void testEventKeyWrite() throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOutput);
        key.write(out);
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOutput.toByteArray()));
        EventKey inKey = new EventKey();
        inKey.readFields(in);
        byteOutput.close();

        Assert.assertEquals(0, key.getSplit().getSegment().compareTo(inKey.getSplit().getSegment()));
        Assert.assertEquals(key.getOffset(), inKey.getOffset());
    }
}
