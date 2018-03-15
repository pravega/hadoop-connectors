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

Run Examples
---

```
Hadoop (verified with Hadoop 2.8.3 on Ubuntu 16.04)

1. setup hadoop/hdfs

2. make sure output dir is empty
   hadoop fs -rmr /tmp/wordcount_output_new_dir/

3. run hadoop command
   hadoop jar build/libs/hadoop-connectors-0.3.0-SNAPSHOT-all.jar io.pravega.examples.hadoop.WordCount tcp://192.168.0.200:9090 myScope myStream /tmp/wordcount_output_new_dir

4. check result
   hadoop fs -cat /tmp/wordcount_output_new_dir/part-r-00000
```

```
Spark (verified with Spark 2.2.1 on Ubuntu 16.04)

spark-submit --class io.pravega.examples.spark.WordCount build/libs/hadoop-connectors-0.3.0-SNAPSHOT-all.jar tcp://192.168.0.200:9090 myScope myStream
```

