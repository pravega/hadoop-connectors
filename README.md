<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# hadoop-connectors
Apache Hadoop connectors for Pravega.

Description
-----------

Implements both the input and the output format interfaces for Hadoop. It leverages Pravega batch client to read existing events in parallel; and uses write API to write events to Pravega stream.

Build
-------
The build script handles Pravega as a _package dependency_, which means the connector is linked to a specific SNAPSHOT version of Pravega (defined at `pravegaVersion` in the `gradle.properties` file). 

### Cloning the repository

```
git clone https://github.com/pravega/hadoop-connectors.git
```

### Building Pravega
Pravega is built automatically by the connector build script.

### Building Hadoop Connector
Build the connector:
```
./gradlew build (w/o dependencies)
./gradlew shadowJar (w/ dependencies)
```

Test
-------
```
./gradlew test
```

Usage
-----
Input Connector
```
        Configuration conf = PravegaInputFormat.builder()
            .withScope("myScope")
            .forStream("myInputStream")
            .withURI("tcp://127.0.0.1:9090")
            .withDeserializer(io.pravega.client.stream.impl.JavaSerializer.class.getName())
            // optional to set start and end positions
            // generally, start positions are set to the end positions in previous job,
            // so only new generated events will be processed, otherwise, start from very beginning if not set
            .startPositions(startPos)
            .endPositions(endPos)
            .build();

        Job job = new Job(conf);
        job.setInputFormatClass(PravegaInputFormat.class);

        // NOTE:
        // 1. You have the option to use existing job 'Configuration' instance as the input parameter to create a builder
        //     "PravegaInputFormat.builder(conf)"
        // 2. Key class is 'EventKey', but you won't need it at most of the time.
```
Output Connector
```
        Configuration conf = PravegaOutputFormat.builder()
            .withScope("myScope")
            .forStream("myOutputStream")
            .withURI("tcp://127.0.0.1:9090")
            .withSerializer(io.pravega.client.stream.impl.JavaSerializer.class.getName())
            // optional to set the scaling of output stream, 1 by default
            .withScaling(3)
            .build();

        Job job = new Job(conf);
        job.setOutputFormatClass(PravegaOutputFormat.class);
        // NOTE:
        // 1. You have the option to use existing job 'Configuration' instance as the output parameter to create a builder
        //     "PravegaOutputFormat.builder(conf)"
```
