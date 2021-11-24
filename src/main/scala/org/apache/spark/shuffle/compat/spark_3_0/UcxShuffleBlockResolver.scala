/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import java.io.{File, RandomAccessFile}

import org.apache.spark.TaskContext
import org.apache.spark.shuffle.ucx.{CommonUcxShuffleBlockResolver, CommonUcxShuffleManager}

/**
 * Mapper entry point for UcxShuffle plugin. Performs memory registration
 * of data and index files and publish addresses to driver metadata buffer.
 */
class UcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
  extends CommonUcxShuffleBlockResolver(ucxShuffleManager) {


  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Long,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    // In Spark-3.0 MapId is long and unique among all jobs in spark. We need to use partitionId as offset
    // in metadata buffer
    val partitionId = TaskContext.getPartitionId()
    val dataFile = getDataFile(shuffleId, mapId)
    if (!dataFile.exists() || dataFile.length() == 0) {
      return
    }
    writeIndexFileAndCommitCommon(shuffleId, partitionId, lengths, new RandomAccessFile(dataFile, "r"))
  }
}
