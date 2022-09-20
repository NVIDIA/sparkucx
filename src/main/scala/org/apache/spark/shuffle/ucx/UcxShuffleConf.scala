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
class UcxShuffleConf(sparkConf: SparkConf) extends SparkConf {

  def getSparkConf: SparkConf = sparkConf

  private def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  // Memory Pool
  private lazy val PREALLOCATE_BUFFERS =
  ConfigBuilder(getUcxConf("memory.preAllocateBuffers"))
    .doc("Comma separated list of buffer size : buffer count pairs to preallocate in memory pool. E.g. 4k:1000,16k:500")
    .stringConf.createWithDefault("")

  lazy val preallocateBuffersMap: Map[Long, Int] = {
    sparkConf.get(PREALLOCATE_BUFFERS).split(",").withFilter(s => s.nonEmpty)
      .map(entry => entry.split(":") match {
        case Array(bufferSize, bufferCount) => (bufferSize.toLong, bufferCount.toInt)
      }).toMap
  }

  private lazy val MIN_BUFFER_SIZE = ConfigBuilder(getUcxConf("memory.minBufferSize"))
    .doc("Minimal buffer size in memory pool.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(4096)

  lazy val minBufferSize: Long = sparkConf.getSizeAsBytes(MIN_BUFFER_SIZE.key,
    MIN_BUFFER_SIZE.defaultValueString)

  private lazy val MIN_REGISTRATION_SIZE =
    ConfigBuilder(getUcxConf("memory.minAllocationSize"))
    .doc("Minimal memory registration size in memory pool.")
    .bytesConf(ByteUnit.MiB)
    .createWithDefault(1)

  lazy val minRegistrationSize: Int = sparkConf.getSizeAsBytes(MIN_REGISTRATION_SIZE.key,
    MIN_REGISTRATION_SIZE.defaultValueString).toInt

  private lazy val SOCKADDR =
    ConfigBuilder(getUcxConf("listener.sockaddr"))
      .doc("Whether to use socket address to connect executors.")
      .stringConf
      .createWithDefault("0.0.0.0:0")

  lazy val listenerAddress: String = sparkConf.get(SOCKADDR.key, SOCKADDR.defaultValueString)

  private lazy val WAKEUP_FEATURE =
    ConfigBuilder(getUcxConf("useWakeup"))
      .doc("Whether to use busy polling for workers")
      .booleanConf
      .createWithDefault(true)

  lazy val useWakeup: Boolean = sparkConf.getBoolean(WAKEUP_FEATURE.key, WAKEUP_FEATURE.defaultValue.get)

  private lazy val NUM_IO_THREADS= ConfigBuilder(getUcxConf("numIoThreads"))
    .doc("Number of threads in io thread pool")
    .intConf
    .createWithDefault(1)

  lazy val numIoThreads: Int = sparkConf.getInt(NUM_IO_THREADS.key, NUM_IO_THREADS.defaultValue.get)

  private lazy val NUM_LISTNER_THREADS= ConfigBuilder(getUcxConf("numListenerThreads"))
    .doc("Number of threads in listener thread pool")
    .intConf
    .createWithDefault(3)

  lazy val numListenerThreads: Int = sparkConf.getInt(NUM_LISTNER_THREADS.key, NUM_LISTNER_THREADS.defaultValue.get)

  private lazy val NUM_WORKERS = ConfigBuilder(getUcxConf("numClientWorkers"))
    .doc("Number of client workers")
    .intConf
    .createWithDefault(1)

  lazy val numWorkers: Int = sparkConf.getInt(NUM_WORKERS.key, sparkConf.getInt("spark.executor.cores",
    NUM_WORKERS.defaultValue.get))

  private lazy val MAX_BLOCKS_IN_FLIGHT = ConfigBuilder(getUcxConf("maxBlocksPerRequest"))
    .doc("Maximum number blocks per request")
    .intConf
    .createWithDefault(50)

  lazy val maxBlocksPerRequest: Int = sparkConf.getInt(MAX_BLOCKS_IN_FLIGHT.key, MAX_BLOCKS_IN_FLIGHT.defaultValue.get)
}
