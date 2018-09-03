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
 * A pravega hadoop output connector builder.
 *
 */
public class PravegaOutputFormatBuilder extends PravegaBuilder<PravegaOutputFormatBuilder> {

    public PravegaOutputFormatBuilder() {}

    public PravegaOutputFormatBuilder(Configuration conf) {
        super(conf);
    }

    @Override
    public void check() {
        String[] paramsToCheck = new String[] {
            PravegaConfig.OUTPUT_URI_STRING,
            PravegaConfig.OUTPUT_SERIALIZER
        };

        for (String param : paramsToCheck) {
            Preconditions.checkArgument(!this.settings.getOrDefault(param, "").isEmpty());
        }

        String stream = this.settings.getOrDefault(PravegaConfig.OUTPUT_STREAM_NAME, "");
        Preconditions.checkArgument(!stream.isEmpty());

        String scope = this.settings.getOrDefault(PravegaConfig.OUTPUT_SCOPE_NAME, "");
        if (scope.isEmpty()) {
            // check if scope is supplied as qualified stream name
            Preconditions.checkArgument(stream.split("/").length == 2);
        }
    }

    @Override
    public PravegaOutputFormatBuilder withScope(String scopeName) {
        this.setString(PravegaConfig.OUTPUT_SCOPE_NAME, scopeName);
        return builder();
    }

    @Override
    public PravegaOutputFormatBuilder forStream(String streamName) {
        this.setString(PravegaConfig.OUTPUT_STREAM_NAME, streamName);
        return builder();
    }

    @Override
    public PravegaOutputFormatBuilder withURI(String uri) {
        this.setString(PravegaConfig.OUTPUT_URI_STRING, uri);
        return builder();
    }

    /** Adds Serializer class name to builder.
     *
     * @param className String
     * @return builder instance
     */
    public PravegaOutputFormatBuilder withSerializer(String className) {
        this.setString(PravegaConfig.OUTPUT_SERIALIZER, className);
        return builder();
    }

    /** Adds PravegaEventRouter class name to builder.
     *
     * @param className String
     * @return builder instance
     */
    public PravegaOutputFormatBuilder withEventRouter(String className) {
        this.setString(PravegaConfig.OUTPUT_EVENT_ROUTER, className);
        return builder();
    }

    /** set scaling of output stream.
     *
     * @param scaling int
     * @return builder instance
     */
    public PravegaOutputFormatBuilder withScaling(int scaling) {
        this.setString(PravegaConfig.OUTPUT_SCALING, String.valueOf(scaling));
        return builder();
    }

    private PravegaOutputFormatBuilder builder() {
        return this;
    }
}
