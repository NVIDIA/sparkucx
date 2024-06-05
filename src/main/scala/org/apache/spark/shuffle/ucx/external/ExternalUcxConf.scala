/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

/**
 * Plugin configuration properties.
 */
trait ExternalUcxConf {
  lazy val preallocateBuffersMap: Map[Long, Int] =
    ExternalUcxConf.preAllocateConfToMap(ExternalUcxConf.PREALLOCATE_BUFFERS_DEFAULT)
  lazy val memoryLimit: Boolean = ExternalUcxConf.MEMORY_LIMIT_DEFAULT
  lazy val memoryGroupSize: Int = ExternalUcxConf.MEMORY_GROUP_SIZE_DEFAULT
  lazy val minBufferSize: Long = ExternalUcxConf.MIN_BUFFER_SIZE_DEFAULT
  lazy val maxBufferSize: Long = ExternalUcxConf.MAX_BUFFER_SIZE_DEFAULT
  lazy val minRegistrationSize: Long = ExternalUcxConf.MIN_REGISTRATION_SIZE_DEFAULT
  lazy val maxRegistrationSize: Long = ExternalUcxConf.MAX_REGISTRATION_SIZE_DEFAULT
  lazy val numPools: Int = ExternalUcxConf.NUM_POOLS_DEFAULT
  lazy val listenerAddress: String = ExternalUcxConf.SOCKADDR_DEFAULT
  lazy val useWakeup: Boolean = ExternalUcxConf.WAKEUP_FEATURE_DEFAULT
  lazy val numIoThreads: Int = ExternalUcxConf.NUM_IO_THREADS_DEFAULT
  lazy val numThreads: Int = ExternalUcxConf.NUM_THREADS_DEFAULT
  lazy val numWorkers: Int = ExternalUcxConf.NUM_WORKERS_DEFAULT
  lazy val maxBlocksPerRequest: Int = ExternalUcxConf.MAX_BLOCKS_IN_FLIGHT_DEFAULT
  lazy val ucxServerPort: Int = ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT
  lazy val maxReplySize: Long = ExternalUcxConf.MAX_REPLY_SIZE_DEFAULT
}

object ExternalUcxConf {
  private[ucx] def getUcxConf(name: String) = s"spark.shuffle.ucx.$name"

  lazy val PREALLOCATE_BUFFERS_KEY = getUcxConf("memory.preAllocateBuffers")
  lazy val PREALLOCATE_BUFFERS_DEFAULT = ""

  lazy val MEMORY_LIMIT_KEY = getUcxConf("memory.limit")
  lazy val MEMORY_LIMIT_DEFAULT = false

  lazy val MEMORY_GROUP_SIZE_KEY = getUcxConf("memory.groupSize")
  lazy val MEMORY_GROUP_SIZE_DEFAULT = 3

  lazy val MIN_BUFFER_SIZE_KEY = getUcxConf("memory.minBufferSize")
  lazy val MIN_BUFFER_SIZE_DEFAULT = 4096L

  lazy val MAX_BUFFER_SIZE_KEY = getUcxConf("memory.maxBufferSize")
  lazy val MAX_BUFFER_SIZE_DEFAULT = Int.MaxValue.toLong

  lazy val MIN_REGISTRATION_SIZE_KEY = getUcxConf("memory.minAllocationSize")
  lazy val MIN_REGISTRATION_SIZE_DEFAULT = 1L * 1024 * 1024

  lazy val MAX_REGISTRATION_SIZE_KEY = getUcxConf("memory.maxAllocationSize")
  lazy val MAX_REGISTRATION_SIZE_DEFAULT = 16L * 1024 * 1024 * 1024

  lazy val NUM_POOLS_KEY = getUcxConf("memory.numPools")
  lazy val NUM_POOLS_DEFAULT = 1

  lazy val SOCKADDR_KEY = getUcxConf("listener.sockaddr")
  lazy val SOCKADDR_DEFAULT = "0.0.0.0:0"

  lazy val WAKEUP_FEATURE_KEY = getUcxConf("useWakeup")
  lazy val WAKEUP_FEATURE_DEFAULT = true

  lazy val NUM_IO_THREADS_KEY = getUcxConf("numIoThreads")
  lazy val NUM_IO_THREADS_DEFAULT = 1

  lazy val NUM_THREADS_KEY = getUcxConf("numThreads")
  lazy val NUM_THREADS_COMPAT_KEY = getUcxConf("numListenerThreads")
  lazy val NUM_THREADS_DEFAULT = 4

  lazy val NUM_WORKERS_KEY = getUcxConf("numWorkers")
  lazy val NUM_WORKERS_COMPAT_KEY = getUcxConf("numClientWorkers")
  lazy val NUM_WORKERS_DEFAULT = 1

  lazy val MAX_BLOCKS_IN_FLIGHT_KEY = getUcxConf("maxBlocksPerRequest")
  lazy val MAX_BLOCKS_IN_FLIGHT_DEFAULT = 50

  lazy val MAX_REPLY_SIZE_KEY = getUcxConf("maxReplySize")
  lazy val MAX_REPLY_SIZE_DEFAULT = 32L * 1024 * 1024

  lazy val SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY = getUcxConf("service.port")
  lazy val SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT = 3338

  def preAllocateConfToMap(conf: String): Map[Long, Int] =
    conf.split(",").withFilter(s => s.nonEmpty).map(entry =>
      entry.split(":") match {
        case Array(bufferSize, bufferCount) => (bufferSize.toLong, bufferCount.toInt)
      }).toMap
}