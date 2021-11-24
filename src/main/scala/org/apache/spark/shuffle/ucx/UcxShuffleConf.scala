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
  object PROTOCOL extends Enumeration {
    val ONE_SIDED, AM = Value
  }

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

  private lazy val PREREGISTER_MEMORY = ConfigBuilder(getUcxConf("memory.preregister"))
    .doc("Whether to do ucp mem map for allocated memory in memory pool")
    .booleanConf.createWithDefault(true)

  lazy val preregisterMemory: Boolean = sparkConf.getBoolean(PREREGISTER_MEMORY.key, PREREGISTER_MEMORY.defaultValue.get)

  private lazy val USE_SOCKADDR =
    ConfigBuilder(getUcxConf("useSockAddr"))
      .doc("Whether to use socket address to connect executors.")
      .booleanConf
      .createWithDefault(true)

  private lazy val SOCKADDR =
    ConfigBuilder(getUcxConf("listener.sockaddr"))
      .doc("Whether to use socket address to connect executors.")
      .stringConf
      .createWithDefault("0.0.0.0:0")

  lazy val useSockAddr: Boolean = sparkConf.getBoolean(USE_SOCKADDR.key, USE_SOCKADDR.defaultValue.get)

  lazy val listenerAddress: String = sparkConf.get(SOCKADDR.key, SOCKADDR.defaultValueString)

  private lazy val WAKEUP_FEATURE =
    ConfigBuilder(getUcxConf("useWakeup"))
      .doc("Whether to use busy polling for workers")
      .booleanConf
      .createWithDefault(true)

  lazy val useWakeup: Boolean = sparkConf.getBoolean(WAKEUP_FEATURE.key, WAKEUP_FEATURE.defaultValue.get)

  private lazy val NUM_PROGRESS_THREADS= ConfigBuilder(getUcxConf("numProgressThreads"))
    .doc("Number of threads in progress thread pool")
    .intConf
    .createWithDefault(3)

  lazy val numProgressThreads: Int = sparkConf.getInt(NUM_PROGRESS_THREADS.key, NUM_PROGRESS_THREADS.defaultValue.get)

  private lazy val NUM_WORKERS = ConfigBuilder(getUcxConf("numWorkers"))
    .doc("Number of client workers")
    .intConf
    .createWithDefault(1)

  lazy val numWorkers: Int = sparkConf.getInt(NUM_WORKERS.key, sparkConf.getInt("spark.executor.cores",
    NUM_WORKERS.defaultValue.get))
}
