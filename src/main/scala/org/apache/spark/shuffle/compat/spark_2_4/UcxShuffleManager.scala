/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import org.apache.spark.shuffle.compat.spark_2_4.{UcxShuffleBlockResolver, UcxShuffleReader}
import org.apache.spark.shuffle.ucx.CommonUcxShuffleManager
import org.apache.spark.{SparkConf, TaskContext}

/**
 * Main entry point of Ucx shuffle plugin. It extends spark's default SortShufflePlugin
 * and injects needed logic in override methods.
 */
class UcxShuffleManager(override val conf: SparkConf, isDriver: Boolean)
  extends CommonUcxShuffleManager(conf, isDriver) {

  /**
   * Mapper callback on executor. Just start UcxNode and use Spark mapper logic.
   */
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int,
                               context: TaskContext): ShuffleWriter[K, V] = {
    super.getWriter(handle, mapId, context)
  }

  override val shuffleBlockResolver: UcxShuffleBlockResolver = new UcxShuffleBlockResolver(this)

  /**
   * Reducer callback on executor.
   */
  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int,
                               endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    new UcxShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K,_,C]], startPartition,
      endPartition, context, ucxTransport)
  }
}

