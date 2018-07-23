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

    /** Adds Deserializer class name to builder.
     *
     * @param className String
     * @return builder instance
     */
    public PravegaInputFormatBuilder withDeserializer(String className) {
        this.setString(PravegaConfig.INPUT_DESERIALIZER, className);
        return builder();
    }

    /**
     * Adds optional start position to builder
     *
     * generally, start position are set to the end position in previous job,
     * so only new generated events will be processed, otherwise, start from very beginning
     * See also {@link #endPosition(String)}.
     *
     * @param startPos String
     * @return builder instance
     */
    public PravegaInputFormatBuilder startPosition(String startPos) {
        this.setString(PravegaConfig.INPUT_START_POSITION, startPos);
        return builder();
    }

    /**
     * Adds optional end position to builder, so the current job will only process events until these end position inclusively
     *
     * The current latest position can be retrieved by below code, and it shall be saved for future reference in most of cases
     * String position = PravegaInputFormat.fetchLatestPosition("tcp://192.168.0.200:9090", "myScope", "myStream").
     *
     * @param endPos String
     * @return builder instance
     */
    public PravegaInputFormatBuilder endPosition(String endPos) {
        this.setString(PravegaConfig.INPUT_END_POSITION, endPos);
        return builder();
    }

    private PravegaInputFormatBuilder builder() {
        return this;
    }
}
