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

public class PravegaConfig {

    public static final String INPUT_SCOPE_NAME = "input.pravega.scope";
    public static final String INPUT_STREAM_NAME = "input.pravega.stream";
    public static final String INPUT_URI_STRING = "input.pravega.uri";
    public static final String INPUT_DESERIALIZER = "input.pravega.deserializer";
    public static final String INPUT_START_POSITIONS = "input.pravega.startpositions";
    public static final String INPUT_END_POSITIONS = "input.pravega.endpositions";

    public static final String OUTPUT_SCOPE_NAME = "output.pravega.scope";
    public static final String OUTPUT_STREAM_NAME = "output.pravega.stream";
    public static final String OUTPUT_URI_STRING = "output.pravega.uri";
    public static final String OUTPUT_SERIALIZER = "output.pravega.serializer";
}
