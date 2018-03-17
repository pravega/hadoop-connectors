# hadoop-connectors
Apache Hadoop connectors for Pravega.

Description
-----------

Implementation of a Hadoop input format for Pravega (with wordcount examples). It leverages Pravega batch client to read all existing events in parallel.

Build
-------
The build script handles Pravega as a _source dependency_, meaning that the connector is linked to a specific commit of Pravega (as opposed to a specific release version) in order to faciliate co-development.  This is accomplished with a combination of a _git submodule_ and the use of Gradle's _composite build_ feature. 

### Cloning the repository
When cloning the connector repository, be sure to instruct git to recursively checkout submodules, e.g.:
```
git clone --recurse-submodules https://github.com/pravega/hadoop-connectors.git
```

To update an existing repository:
```
git submodule update --init --recursive
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
```
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, "myScope");
        conf.setStrings(PravegaInputFormat.STREAM_NAME, "myStream");
        conf.setStrings(PravegaInputFormat.URI_STRING, "tcp://127.0.0.1:9090");
        conf.setStrings(PravegaInputFormat.DESERIALIZER, io.pravega.client.stream.impl.JavaSerializer.class.getName());

        Job job = new Job(conf);
        job.setInputFormatClass(PravegaInputFormat.class);

        // FYI, Key class is 'EventKey', but you won't need it at most of the time.
```
