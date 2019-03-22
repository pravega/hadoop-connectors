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

    @Before
    public void initialize() {
        environmentVariables.set("pravega_client_auth_method", "true");
        environmentVariables.set("pravega_client_auth_token", SetupUtils.defaultAuthToken());
    }

    public void addSecurityConfiguration(Configuration conf, SetupUtils setupUtils) throws IOException {

        if (setupUtils.isEnableAuth()) {
            conf.set("pravega_client_auth_method", "Default");
            conf.set("pravega_client_auth_token", SetupUtils.defaultAuthToken());
        }

        if (setupUtils.isEnableTls()) {
            File file = new File(setupUtils.getTrustStoreCertFile());
            byte[] trustStore = Files.readAllBytes(file.toPath());
            conf.set(PravegaConfig.BASE64_TRUSTSTORE_FILE, Base64.getEncoder().encodeToString(trustStore));
        }
    }
}
