/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_2_4

import java.io.{File, RandomAccessFile}

import org.apache.spark.shuffle.ucx.{CommonUcxShuffleBlockResolver, CommonUcxShuffleManager}

/**
 * Mapper entry point for UcxShuffle plugin. Performs memory registration
 * of data and index files and publish addresses to driver metadata buffer.
 */
class UcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
  extends CommonUcxShuffleBlockResolver(ucxShuffleManager) {

  /**
   * Mapper commit protocol extension. Register index and data files and publish all needed
   * metadata to driver.
   */
  override def writeIndexFileAndCommit(shuffleId: ShuffleId, mapId: Int,
                                       lengths: Array[Long], dataTmp: File): Unit = {
    super.writeIndexFileAndCommit(shuffleId, mapId, lengths, dataTmp)
    val dataFile = getDataFile(shuffleId, mapId)
    if (!dataFile.exists() || dataFile.length() == 0) {
      return
    }

    writeIndexFileAndCommitCommon(shuffleId, mapId,  lengths,  new RandomAccessFile(dataFile, "r"))
  }
}
