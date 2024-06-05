/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

/**
 * Plugin configuration properties.
 */
class ExternalUcxClientConf(val sparkConf: SparkConf) extends SparkConf with ExternalUcxConf {

  def getSparkConf: SparkConf = sparkConf

  // Memory Pool
  private lazy val PREALLOCATE_BUFFERS =
  ConfigBuilder(ExternalUcxConf.PREALLOCATE_BUFFERS_KEY)
    .doc("Comma separated list of buffer size : buffer count pairs to preallocate in memory pool. E.g. 4k:1000,16k:500")
    .stringConf.createWithDefault("")

  override lazy val preallocateBuffersMap: Map[Long, Int] = 
    ExternalUcxConf.preAllocateConfToMap(
      sparkConf.get(ExternalUcxConf.PREALLOCATE_BUFFERS_KEY,
                    ExternalUcxConf.PREALLOCATE_BUFFERS_DEFAULT))

  private lazy val MEMORY_LIMIT = ConfigBuilder(ExternalUcxConf.MEMORY_LIMIT_KEY)
    .doc("Enable memory pool size limit.")
    .booleanConf
    .createWithDefault(ExternalUcxConf.MEMORY_LIMIT_DEFAULT)

  override lazy val memoryLimit: Boolean = sparkConf.getBoolean(MEMORY_LIMIT.key,
    MEMORY_LIMIT.defaultValue.get)

  private lazy val MEMORY_GROUP_SIZE = ConfigBuilder(ExternalUcxConf.MEMORY_GROUP_SIZE_KEY)
    .doc("Memory group size.")
    .intConf
    .createWithDefault(ExternalUcxConf.MEMORY_GROUP_SIZE_DEFAULT)

  override lazy val memoryGroupSize: Int = sparkConf.getInt(MEMORY_GROUP_SIZE.key,
    MEMORY_GROUP_SIZE.defaultValue.get)

  private lazy val MIN_BUFFER_SIZE = ConfigBuilder(ExternalUcxConf.MIN_BUFFER_SIZE_KEY)
    .doc("Minimal buffer size in memory pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(ExternalUcxConf.MIN_BUFFER_SIZE_DEFAULT)

  override lazy val minBufferSize: Long = sparkConf.getSizeAsBytes(MIN_BUFFER_SIZE.key,
    MIN_BUFFER_SIZE.defaultValue.get)

  private lazy val MAX_BUFFER_SIZE = ConfigBuilder(ExternalUcxConf.MAX_BUFFER_SIZE_KEY)
    .doc("Maximal buffer size in memory pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(ExternalUcxConf.MAX_BUFFER_SIZE_DEFAULT)

  override lazy val maxBufferSize: Long = sparkConf.getSizeAsBytes(MAX_BUFFER_SIZE.key,
    MAX_BUFFER_SIZE.defaultValue.get)

  private lazy val MIN_REGISTRATION_SIZE =
    ConfigBuilder(ExternalUcxConf.MIN_REGISTRATION_SIZE_KEY)
    .doc("Minimal memory registration size in memory pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(ExternalUcxConf.MIN_REGISTRATION_SIZE_DEFAULT)

  override lazy val minRegistrationSize: Long = sparkConf.getSizeAsBytes(MIN_REGISTRATION_SIZE.key,
    MIN_REGISTRATION_SIZE.defaultValue.get).toInt

  private lazy val MAX_REGISTRATION_SIZE =
    ConfigBuilder(ExternalUcxConf.MAX_REGISTRATION_SIZE_KEY)
    .doc("Maximal memory registration size in memory pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(ExternalUcxConf.MAX_REGISTRATION_SIZE_DEFAULT)

  override lazy val maxRegistrationSize: Long = sparkConf.getSizeAsBytes(MAX_REGISTRATION_SIZE.key,
    MAX_REGISTRATION_SIZE.defaultValue.get).toLong

  private lazy val NUM_POOLS =
    ConfigBuilder(ExternalUcxConf.NUM_POOLS_KEY)
    .doc("Number of memory pool.")
    .intConf
    .createWithDefault(ExternalUcxConf.NUM_POOLS_DEFAULT)

  override lazy val numPools: Int = sparkConf.getInt(NUM_POOLS.key,
    NUM_POOLS.defaultValue.get)

  private lazy val SOCKADDR =
    ConfigBuilder(ExternalUcxConf.SOCKADDR_KEY)
      .doc("Whether to use socket address to connect executors.")
      .stringConf
      .createWithDefault(ExternalUcxConf.SOCKADDR_DEFAULT)

  override lazy val listenerAddress: String = sparkConf.get(SOCKADDR.key, SOCKADDR.defaultValueString)

  private lazy val WAKEUP_FEATURE =
    ConfigBuilder(ExternalUcxConf.WAKEUP_FEATURE_KEY)
      .doc("Whether to use busy polling for workers")
      .booleanConf
      .createWithDefault(ExternalUcxConf.WAKEUP_FEATURE_DEFAULT)

  override lazy val useWakeup: Boolean = sparkConf.getBoolean(WAKEUP_FEATURE.key, WAKEUP_FEATURE.defaultValue.get)

  private lazy val NUM_WORKERS = ConfigBuilder(ExternalUcxConf.NUM_WORKERS_KEY)
    .doc("Number of client workers")
    .intConf
    .createWithDefault(ExternalUcxConf.NUM_WORKERS_DEFAULT)

  override lazy val numWorkers: Int = sparkConf.getInt(NUM_WORKERS.key, sparkConf.getInt("spark.executor.cores",
    NUM_WORKERS.defaultValue.get))

  private lazy val NUM_THREADS= ConfigBuilder(ExternalUcxConf.NUM_THREADS_KEY)
    .doc("Number of threads in thread pool")
    .intConf
    .createWithDefault(ExternalUcxConf.NUM_THREADS_DEFAULT)

  override lazy val numThreads: Int = sparkConf.getInt(NUM_THREADS.key,
    sparkConf.getInt(ExternalUcxConf.NUM_THREADS_COMPAT_KEY, NUM_THREADS.defaultValue.get))

  private lazy val MAX_BLOCKS_IN_FLIGHT = ConfigBuilder(ExternalUcxConf.MAX_BLOCKS_IN_FLIGHT_KEY)
    .doc("Maximum number blocks per request")
    .intConf
    .createWithDefault(ExternalUcxConf.MAX_BLOCKS_IN_FLIGHT_DEFAULT)

  override lazy val maxBlocksPerRequest: Int = sparkConf.getInt(MAX_BLOCKS_IN_FLIGHT.key, MAX_BLOCKS_IN_FLIGHT.defaultValue.get)

  private lazy val MAX_REPLY_SIZE = ConfigBuilder(ExternalUcxConf.MAX_REPLY_SIZE_KEY)
    .doc("Maximum number blocks per request")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(ExternalUcxConf.MAX_REPLY_SIZE_DEFAULT)

  override lazy val maxReplySize: Long = sparkConf.getSizeAsBytes(MAX_REPLY_SIZE.key, MAX_REPLY_SIZE.defaultValue.get)

  override lazy val ucxServerPort: Int = sparkConf.getInt(
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY,
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT)
}