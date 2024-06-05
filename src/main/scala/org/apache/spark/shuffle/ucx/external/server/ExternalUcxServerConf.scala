/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.SparkConf

/**
 * Plugin configuration properties.
 */
class ExternalUcxServerConf(val yarnConf: Configuration) extends ExternalUcxConf {
  override lazy val preallocateBuffersMap: Map[Long, Int] =
    ExternalUcxConf.preAllocateConfToMap(
      yarnConf.get(ExternalUcxConf.PREALLOCATE_BUFFERS_KEY,
                   ExternalUcxConf.PREALLOCATE_BUFFERS_DEFAULT))

  override lazy val memoryLimit: Boolean = yarnConf.getBoolean(
    ExternalUcxConf.MEMORY_LIMIT_KEY,
    ExternalUcxConf.MEMORY_LIMIT_DEFAULT)

  override lazy val memoryGroupSize: Int = yarnConf.getInt(
    ExternalUcxConf.MEMORY_GROUP_SIZE_KEY,
    ExternalUcxConf.MEMORY_GROUP_SIZE_DEFAULT)

  override lazy val minBufferSize: Long = yarnConf.getLong(
    ExternalUcxConf.MIN_BUFFER_SIZE_KEY,
    ExternalUcxConf.MIN_BUFFER_SIZE_DEFAULT)

  override lazy val maxBufferSize: Long = yarnConf.getLong(
    ExternalUcxConf.MAX_BUFFER_SIZE_KEY,
    ExternalUcxConf.MAX_BUFFER_SIZE_DEFAULT)

  override lazy val minRegistrationSize: Long = yarnConf.getLong(
    ExternalUcxConf.MIN_REGISTRATION_SIZE_KEY,
    ExternalUcxConf.MIN_REGISTRATION_SIZE_DEFAULT)

  override lazy val maxRegistrationSize: Long = yarnConf.getLong(
    ExternalUcxConf.MAX_REGISTRATION_SIZE_KEY,
    ExternalUcxConf.MAX_REGISTRATION_SIZE_DEFAULT)

  override lazy val numPools: Int = yarnConf.getInt(
    ExternalUcxConf.NUM_POOLS_KEY,
    ExternalUcxConf.NUM_POOLS_DEFAULT)

  override lazy val listenerAddress: String = yarnConf.get(
    ExternalUcxConf.SOCKADDR_KEY,
    ExternalUcxConf.SOCKADDR_DEFAULT)

  override lazy val useWakeup: Boolean = yarnConf.getBoolean(
    ExternalUcxConf.WAKEUP_FEATURE_KEY,
    ExternalUcxConf.WAKEUP_FEATURE_DEFAULT)

  override lazy val numWorkers: Int = yarnConf.getInt(
    ExternalUcxConf.NUM_WORKERS_KEY,
    yarnConf.getInt(
      ExternalUcxConf.NUM_WORKERS_COMPAT_KEY,
      ExternalUcxConf.NUM_WORKERS_DEFAULT))

  override lazy val numThreads: Int = yarnConf.getInt(
    ExternalUcxConf.NUM_THREADS_KEY,
    yarnConf.getInt(
      ExternalUcxConf.NUM_THREADS_COMPAT_KEY,
      ExternalUcxConf.NUM_THREADS_DEFAULT))

  override lazy val ucxServerPort: Int = yarnConf.getInt(
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_KEY,
    ExternalUcxConf.SPARK_UCX_SHUFFLE_SERVICE_PORT_DEFAULT)

  override lazy val maxReplySize: Long = yarnConf.getLong(
    ExternalUcxConf.MAX_REPLY_SIZE_KEY,
    ExternalUcxConf.MAX_REPLY_SIZE_DEFAULT)

  lazy val ucxEpsNum: Int = yarnConf.getInt(
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_EPS_NUM_KEY,
    ExternalUcxServerConf.SPARK_UCX_SHUFFLE_EPS_NUM_DEFAULT)
}

object ExternalUcxServerConf {
  lazy val SPARK_UCX_SHUFFLE_SERVICE_TCP_PORT_KEY = ExternalUcxConf.getUcxConf("service.tcp.port")

  lazy val SPARK_UCX_SHUFFLE_EPS_NUM_KEY = ExternalUcxConf.getUcxConf("eps.num")
  lazy val SPARK_UCX_SHUFFLE_EPS_NUM_DEFAULT = 16777216
}