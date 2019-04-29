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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.UUID;

public class SecurityHelper {

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    public static String decodeTrustStoreDataToTempFile(String base64EncodedTrustStoreContent) throws IOException {
        Preconditions.checkNotNull(base64EncodedTrustStoreContent, "Base64 encoded truststore content cannot be null");
        byte[] decodedTrustStoreContent = Base64.getDecoder().decode(base64EncodedTrustStoreContent);
        StringBuilder path = new StringBuilder();
        final String randomId = UUID.randomUUID().toString();
        path.append(TEMP_DIR).append(File.separator).append(randomId).append(".pem");
        try (FileOutputStream outputStream = new FileOutputStream(path.toString())) {
            outputStream.write(decodedTrustStoreContent);
        }
        return path.toString();
    }

    public static ClientConfig prepareClientConfig(Configuration conf, URI controllerURI) throws IOException {
        PravegaClientConfig pravegaClientConfig = PravegaClientConfig.fromDefaults();
        pravegaClientConfig.withControllerURI(controllerURI);

        boolean validateHostName = conf.getBoolean(PravegaConfig.VALIDATE_HOST_NAME, false);
        pravegaClientConfig.withHostnameValidation(validateHostName);

        String base64EncodedTrustStoreContent = conf.get(PravegaConfig.BASE64_TRUSTSTORE_FILE);
        if (base64EncodedTrustStoreContent != null && base64EncodedTrustStoreContent.length() != 0) {
            pravegaClientConfig.withTrustStore(decodeTrustStoreDataToTempFile(base64EncodedTrustStoreContent));
        }
        return pravegaClientConfig.getClientConfig();
    }
}
