# UCX for Apache Spark Plugin
UCX for Apache Spark is a high performance ShuffleManager plugin for Apache Spark, that uses RDMA and other high performance transports
that are supported by [UCX](https://github.com/openucx/ucx#supported-transports), to perform Shuffle data transfers in Spark jobs.

## Runtime requirements
* Apache Spark 2.3/2.4/3.0
* Java 8+
* Installed UCX of version 1.10+, and [UCX supported transport hardware](https://github.com/openucx/ucx#supported-transports).

## Installation

### Obtain UCX for Apache Spark
Please use the ["Releases"](https://github.com/NVIDIA/sparkucx/releases) page to download SparkUCX jar file
for your spark version (e.g. ucx-spark-1.0-for-spark-2.4.0-jar-with-dependencies.jar).
Put ucx-spark jar file in $SPARK_UCX_HOME on all the nodes in your cluster.
<br>If you would like to build the project yourself, please refer to the ["Build"](https://github.com/NVIDIA/sparkucx#build) section below.

Ucx binaries **must** be in Spark classpath on every Spark Master and Worker.
It can be obtained by installing the latest version from [Ucx release page](https://github.com/openucx/ucx/releases)

### Configuration

Provide Spark the location of the SparkUCX plugin jars and ucx shared binaries by using the extraClassPath option.

```
spark.driver.extraClassPath     $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:$UCX_PREFIX/lib
spark.executor.extraClassPath   $SPARK_UCX_HOME/spark-ucx-1.0-for-spark-2.4.0-jar-with-dependencies.jar:$UCX_PREFIX/lib
```
To enable the UCX for Apache Spark Shuffle Manager plugin, add the following configuration property
to spark (e.g. in $SPARK_HOME/conf/spark-defaults.conf):

```
spark.shuffle.manager   org.apache.spark.shuffle.UcxShuffleManager
```
For spark-3.0 version add SparkUCX ShuffleIO plugin:
```
spark.shuffle.sort.io.plugin.class org.apache.spark.shuffle.compat.spark_3_0.UcxLocalDiskShuffleDataIO
```

### Build

Building the SparkUCX plugin requires [Apache Maven](http://maven.apache.org/) and Java 8+ JDK

Build instructions:

```
% git clone https://github.com/openucx/sparkucx
% cd sparkucx
% mvn -DskipTests clean package -Pspark-2.4
```

### Performance

UCX for Apache Spark plugin is built to provide the best performance out-of-the-box, and provides multiple configuration options to further tune UCX for Apache Spark per-job. 
For more information on how to setup [HiBench](https://github.com/Intel-bigdata/HiBench) benchmark and reproduce results, please refer to [Accelerated Apache SparkUCX 2.4/3.0 cluster deployment](https://docs.mellanox.com/pages/releaseview.action?pageId=19819236).

![Performance results](https://docs.mellanox.com/download/attachments/19819236/image2020-1-23_15-39-14.png)

