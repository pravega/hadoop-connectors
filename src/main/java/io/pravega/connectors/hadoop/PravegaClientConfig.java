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
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.Credentials;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.Data;

/**
 * SecurityHelper class to assist building Pravega client configurations.
 */
public class PravegaClientConfig {

    static final PravegaParameter CONTROLLER_PARAM = new PravegaParameter("controller", "pravega.controller.uri", "PRAVEGA_CONTROLLER_URI");
    static final PravegaParameter SCOPE_PARAM = new PravegaParameter("scope", "pravega.scope", "PRAVEGA_SCOPE");

    private static final long serialVersionUID = 1L;

    private URI controllerURI;
    private String defaultScope;
    private Credentials credentials;
    private boolean validateHostname = true;
    private String trustStore;

    PravegaClientConfig(Properties properties, Map<String, String> env, Map<String, String> params) {
        Preconditions.checkNotNull(properties, "properties cannot be null");
        Preconditions.checkNotNull(env, "env cannot be null");
        Preconditions.checkNotNull(params, "params cannot be null");
        this.controllerURI = CONTROLLER_PARAM.resolve(params, properties, env).map(URI::create).orElse(null);
        this.defaultScope = SCOPE_PARAM.resolve(params, properties, env).orElse(null);
    }

    /**
     * Gets a configuration based on defaults obtained from the local environment.
     */
    public static PravegaClientConfig fromDefaults() {
        return new PravegaClientConfig(System.getProperties(), System.getenv(), new HashMap<>());
    }

    /**
     * Gets a configuration based on defaults obtained from the local environment plus the given program parameters.
     *
     * @param params the parameters to use.
     */
    public static PravegaClientConfig fromParams(Map<String, String> params) {
        return new PravegaClientConfig(System.getProperties(), System.getenv(), params);
    }

    /**
     * Gets the {@link ClientConfig} to use with the Pravega client.
     */
    public ClientConfig getClientConfig() {
        ClientConfig.ClientConfigBuilder builder = ClientConfig.builder()
                .validateHostName(validateHostname);
        if (controllerURI != null) {
            builder.controllerURI(controllerURI);
        }
        if (credentials != null) {
            builder.credentials(credentials);
        }
        if (trustStore != null) {
            builder.trustStore(trustStore);
        }
        return builder.build();
    }

    /**
     * Configures the Pravega controller RPC URI.
     *
     * @param controllerURI The URI.
     */
    public PravegaClientConfig withControllerURI(URI controllerURI) {
        this.controllerURI = controllerURI;
        return this;
    }

    /**
     * Configures truststore value.
     * @param trustStore truststore name.
     * @return current instance of PravegaClientConfig.
     */
    public PravegaClientConfig withTrustStore(String trustStore) {
        this.trustStore = trustStore;
        return this;
    }

    /**
     * Configures the default Pravega scope, to resolve unqualified stream names and to support reader groups.
     *
     * @param scope The scope to use (with lowest priority).
     */
    public PravegaClientConfig withDefaultScope(String scope) {
        this.defaultScope = scope;
        return this;
    }

    /**
     * Gets the default Pravega scope.
     */
    @Nullable
    public String getDefaultScope() {
        return defaultScope;
    }

    /**
     * Configures the Pravega credentials to use.
     *
     * @param credentials a credentials object.
     */
    public PravegaClientConfig withCredentials(Credentials credentials) {
        this.credentials = credentials;
        return this;
    }

    /**
     * Enables or disables TLS hostname validation (default: true).
     *
     * @param validateHostname a boolean indicating whether to validate the hostname on incoming requests.
     */
    public PravegaClientConfig withHostnameValidation(boolean validateHostname) {
        this.validateHostname = validateHostname;
        return this;
    }

    /**
     * A configuration parameter resolvable via command-line parameters, system properties, or OS environment variables.
     */
    @Data
    static class PravegaParameter {

        private static final long serialVersionUID = 1L;

        private final String parameterName;
        private final String propertyName;
        private final String variableName;

        public Optional<String> resolve(Map<String, String> parameters, Properties properties, Map<String, String> variables) {
            if (parameters != null && parameters.containsKey(parameterName)) {
                return Optional.of(parameters.get(parameterName));
            }
            if (properties != null && properties.containsKey(propertyName)) {
                return Optional.of(properties.getProperty(propertyName));
            }
            if (variables != null && variables.containsKey(variableName)) {
                return Optional.of(variables.get(variableName));
            }
            return Optional.empty();
        }
    }
}