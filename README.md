# UCX for Apache Spark Plugin
UCX for Apache Spark is a high performance ShuffleManager plugin for Apache Spark, that uses RDMA and other high performance transports
that are supported by [UCX](https://github.com/openucx/ucx#supported-transports), to perform Shuffle data transfers in Spark jobs.

## Runtime requirements
* Apache Spark 2.4/3.0
* Java 8+
* Installed UCX of version 1.12+, and [UCX supported transport hardware](https://github.com/openucx/ucx#supported-transports).

## Installation

### Obtain UCX for Apache Spark
Please use the ["Releases"](https://github.com/NVIDIA/sparkucx/releases) page to download SparkUCX jar file
for your spark version (e.g. ucx-spark-1.1-for-spark-2.4.0-jar-with-dependencies.jar).
Put ucx-spark jar file in $SPARK_UCX_HOME on all the nodes in your cluster.
<br>If you would like to build the project yourself, please refer to the ["Build"](https://github.com/NVIDIA/sparkucx#build) section below.

Ucx binaries **must** be in Spark classpath on every Spark Worker.
It can be obtained by installing the latest version from [Ucx release page](https://github.com/openucx/ucx/releases)

### Configuration

Provide Spark the location of the SparkUCX plugin jars and ucx shared binaries by using the extraClassPath option.

```
spark.driver.extraClassPath     $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar
spark.executor.extraClassPath   $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:$UCX_PREFIX/lib
```
To enable the UCX for Apache Spark Shuffle Manager plugin, add the following configuration property
to spark (e.g. in $SPARK_HOME/conf/spark-defaults.conf):

```
spark.shuffle.manager   org.apache.spark.shuffle.UcxShuffleManager
spark.executorEnv.UCX_ERROR_SIGNALS "" 
```


### Build

Building the SparkUCX plugin requires [Apache Maven](http://maven.apache.org/) and Java 8+ JDK

Build instructions:

```
% git clone https://github.com/nvidia/sparkucx
% cd sparkucx
% mvn -DskipTests clean package -Pspark-3.0
```

