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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class PravegaInputSplitTest {

    private static final String scope = "scope";
    private static final String stream = "stream";
    private Segment segment;
    private PravegaInputSplit split;

    @Before
    public void setUp() {
        segment = new Segment(scope, stream, 10);
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
}
