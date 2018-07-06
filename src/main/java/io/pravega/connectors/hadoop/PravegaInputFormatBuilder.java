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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;

/**
 * A pravega hadoop input connector builder.
 *
 */
public class PravegaInputFormatBuilder extends PravegaBuilder<PravegaInputFormatBuilder> {

    public PravegaInputFormatBuilder() {}

    public PravegaInputFormatBuilder(Configuration conf) {
        super(conf);
    }

    @Override
    public void check() {
        String[] paramsToCheck = new String[]{
            PravegaConfig.INPUT_SCOPE_NAME,
            PravegaConfig.INPUT_STREAM_NAME,
            PravegaConfig.INPUT_URI_STRING,
            PravegaConfig.INPUT_DESERIALIZER};

        for (String param : paramsToCheck) {
            Preconditions.checkArgument(!this.settings.getOrDefault(param, "").isEmpty());
        }
    }

    @Override
    public PravegaInputFormatBuilder withScope(String scopeName) {
        this.setString(PravegaConfig.INPUT_SCOPE_NAME, scopeName);
        return builder();
    }

    @Override
    public PravegaInputFormatBuilder forStream(String streamName) {
        this.setString(PravegaConfig.INPUT_STREAM_NAME, streamName);
        return builder();
    }

    @Override
    public PravegaInputFormatBuilder withURI(String uri) {
        this.setString(PravegaConfig.INPUT_URI_STRING, uri);
        return builder();
    }

    @Override
    public PravegaInputFormatBuilder withDeserializer(String className) {
        this.setString(PravegaConfig.INPUT_DESERIALIZER, className);
        return builder();
    }

    @Override
    public PravegaInputFormatBuilder startPositions(String startPos) {
        this.setString(PravegaConfig.INPUT_START_POSITIONS, startPos);
        return builder();
    }

    @Override
    public PravegaInputFormatBuilder endPositions(String endPos) {
        this.setString(PravegaConfig.INPUT_END_POSITIONS, endPos);
        return builder();
    }

    private PravegaInputFormatBuilder builder() {
        return this;
    }
}
