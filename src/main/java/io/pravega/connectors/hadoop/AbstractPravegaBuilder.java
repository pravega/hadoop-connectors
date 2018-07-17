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

import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract pravega hadoop connector builder.
 *
 * The builder is abstracted to act as the base for both the {@link PravegaInputFormatBuilder} builder.
 *
 * @param <B> the builder type.
 */
abstract class PravegaBuilder<B extends PravegaBuilder> {

    protected Map<String, String> settings = new HashMap<>();

    private Configuration conf;

    public PravegaBuilder() {}

    public PravegaBuilder(Configuration conf) {
        this.conf = conf;
    }

    /**
     * build Configuration from key value pairs.
     *
     * @return Configuration associated with pravega hadoop connectors' settings.
     */
    public Configuration build() {
        check();
        if (this.conf == null) {
            this.conf = new Configuration();
        }
        for (Map.Entry<String, String> entry : this.settings.entrySet()) {
            this.conf.setStrings(entry.getKey(), entry.getValue());
        }
        return this.conf;
    }

    /**
     * register key value pair
     *
     * @param param parameter name from {@link PravegaConfig}.
     * @param value
     */

    protected void setString(String param, String value) {
        this.settings.put(param, value);
    }

    /**
     * Adds scope name to builder
     *
     * @param scopeName String
     * @return builder instance
     */
    public abstract B withScope(String scopeName);

    /**
     * Adds stream name to builder
     *
     * @param streamName String
     * @return builder instance
     */
    public abstract B forStream(String streamName);

    /**
     * Adds URI to builder
     *
     * @param uri String
     * @return builder instance
     */
    public abstract B withURI(String uri);

    /**
     * Adds Deserializer class name to builder
     *
     * @param className String
     * @return builder instance
     */
    public abstract B withDeserializer(String className);

    /**
     * Adds optional start positions to builder
     *
     * @param startPos String
     * @return builder instance
     */
    public abstract B startPositions(String startPos);

    /**
     * Adds optional end positions to builder
     *
     * @param endPos String
     * @return builder instance
     */
    public abstract B endPositions(String endPos);
    
    /**
     * Do verification
     *
     */
    public abstract void check();
}
