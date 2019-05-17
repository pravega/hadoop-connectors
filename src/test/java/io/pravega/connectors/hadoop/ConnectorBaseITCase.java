/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.hadoop;

import io.pravega.connectors.hadoop.utils.SetupUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;

public abstract class ConnectorBaseITCase {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    protected final SetupUtils setupUtils;

    public ConnectorBaseITCase() {
        this.setupUtils = new SetupUtils();
    }

    /**
     * Configure security credentials information as environment variables for the Pravega ClientConfig to
     * make use of it to initialize the default Credentials instance.
     */
    @Before
    public void initialize() {
        if (setupUtils.isEnableAuth()) {
            environmentVariables.set("pravega_client_auth_method", "basic");
            environmentVariables.set("pravega_client_auth_token", SetupUtils.defaultAuthToken());
        }
    }

    public void addSecurityConfiguration(Configuration conf) throws IOException {
        if (setupUtils.isEnableTls()) {
            File file = new File(setupUtils.getTrustStoreCertFile());
            byte[] trustStore = Files.readAllBytes(file.toPath());
            conf.set(PravegaConfig.BASE64_TRUSTSTORE_FILE, Base64.getEncoder().encodeToString(trustStore));
        }
    }
}
