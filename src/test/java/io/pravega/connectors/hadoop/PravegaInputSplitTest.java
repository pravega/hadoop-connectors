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

public class PravegaInputSplitTest {

    private static final String TEST_SCOPE = "PravegaInputSplitTest";
    private static final String TEST_STREAM = "stream";

    private Segment segment;
    private PravegaInputSplit split;

    @Before
    public void setUp() {
        segment = new Segment(TEST_SCOPE, TEST_STREAM, 10);
        split = new PravegaInputSplit(segment, 1, 100);
    }

    @Test
    public void testPravegaInputSplitGet() throws IOException {
        Assert.assertEquals(1, split.getStartOffset());
        Assert.assertEquals(100, split.getEndOffset());
        Assert.assertEquals(segment, split.getSegment());
    }

    @Test
    public void testPravegaInputSplitWritable() throws IOException {
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOutput);
        split.write(out);
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOutput.toByteArray()));
        PravegaInputSplit inSplit = new PravegaInputSplit();
        inSplit.readFields(in);
        byteOutput.close();

        Assert.assertEquals(0, split.getSegment().compareTo(inSplit.getSegment()));
        Assert.assertEquals(split.getStartOffset(), inSplit.getStartOffset());
        Assert.assertEquals(split.getEndOffset(), inSplit.getEndOffset());
    }

    @Test
    public void testPravegaInputSplitComparable() throws IOException {
        Segment segment1 = new Segment(TEST_SCOPE, TEST_STREAM, 10);
        PravegaInputSplit split1 = new PravegaInputSplit(segment1, 50, 70);

        for (int seg = 9; seg <= 11; seg++) {
            for (long start = 49; start <= 51; start++) {
                for (long end = 69; end <= 71; end++) {
                    Segment segment2 = new Segment(TEST_SCOPE, TEST_STREAM, seg);
                    PravegaInputSplit split2 = new PravegaInputSplit(segment2, start, end);
                    if (segment1.compareTo(segment2) == 0) {
                        if (50 == start) {
                            Assert.assertTrue(split1.compareTo(split2) == Long.compare(70, end));
                        } else {
                            Assert.assertTrue(split1.compareTo(split2) == Long.compare(50, start));
                        }
                    } else {
                        Assert.assertTrue(split1.compareTo(split2) == segment1.compareTo(segment2));
                    }
                }
            }
        }
    }
}
