# hadoop-connectors
Apache Hadoop connectors for Pravega.

Description
-----------

Implementation of PravegaInputFormat (with wordcount examples). It leverages Pravega batch client to read all existing events in parallel


Build
-------

### Building Pravega

Install the Pravega client libraries to your local Maven repository:
```
$ git clone https://github.com/pravega/pravega.git
$./gradlew install
```

### Building PravegaInputFormat
```
gradle build (w/o dependencies)
gradle shadowJar (w/ dependencies)
```

Test
-------
```
gradle test
```

Usage
-----
```
        Configuration conf = new Configuration();
        conf.setStrings(PravegaInputFormat.SCOPE_NAME, "myScope");
        conf.setStrings(PravegaInputFormat.STREAM_NAME, "myStream");
        conf.setStrings(PravegaInputFormat.URI_STRING, "tcp://127.0.0.1:9090");

        // optional
        conf.setBoolean(PravegaInputFormat.DEBUG, true);

        // optional: default JavaSerializer is provided for Value class which implements 'java.io.Serializable',
        // otherwise, needs to set DESERIALIZER class implementing 'io.pravega.client.stream.Serializer' interface for Value class,
        // Where 'Value' is the event stored in Pravega
        //
        conf.setStrings(PravegaInputFormat.DESERIALIZER, MySerializer.class.getName());

        Job job = new Job(conf);
        job.setInputFormatClass(PravegaInputFormat.class);

        // FYI, Key class is 'MetadataWritable', but you won't need it at most of time.
```

Run Examples
---

```
Hadoop (verified with Hadoop 2.8.1 on Ubuntu 16.04)

HADOOP_CLASSPATH=build/libs/hadoop-common-0.2.0-SNAPSHOT-all.jar HADOOP_USER_CLASSPATH_FIRST=true hadoop jar build/libs/hadoop-common-0.2.0-SNAPSHOT-all.jar io.pravega.examples.hadoop.WordCount tcp://192.168.0.200:9090 myScope myStream /tmp/wordcount_output_new_dir
```

```
Spark (verified with Spark 2.2.0 on Ubuntu 16.04)

spark-submit --conf spark.driver.userClassPathFirst=true --class io.pravega.examples.spark.WordCount build/libs/hadoop-common-0.2.0-SNAPSHOT-all.jar tcp://192.168.0.200:9090 myScope myStream
```
