/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle

import org.apache.spark.shuffle.compat.spark_3_0.{ExternalUcxShuffleClient, ExternalUcxShuffleReader}
import org.apache.spark.shuffle.ucx.ExternalBaseUcxShuffleManager
import org.apache.spark.{SparkConf, TaskContext}

/**
 * Common part for all spark versions for UcxShuffleManager logic
 */
class ExternalUcxShuffleManager(override val conf: SparkConf, isDriver: Boolean)
  extends ExternalBaseUcxShuffleManager(conf, isDriver) {
  private[spark] lazy val transport = awaitUcxTransport
  private[spark] lazy val shuffleClient = new ExternalUcxShuffleClient(transport)
  override def getReader[K, C](
    handle: ShuffleHandle, startPartition: MapId, endPartition: MapId,
    context: TaskContext, metrics: ShuffleReadMetricsReporter):
      ShuffleReader[K, C] = {
    new ExternalUcxShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K,_,C]],
                                 startPartition, endPartition, context,
                                 shuffleClient, metrics, shouldBatchFetch = false)
  }
}