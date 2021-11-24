/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.compat.spark_3_0

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, BlockStoreClient, DownloadFileManager}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class UcxShuffleClient(val transport: UcxShuffleTransport, mapId2PartitionId: Map[Long, Int]) extends BlockStoreClient with Logging {

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val ucxBlockIds = Array.ofDim[UcxShuffleBockId](blockIds.length)
    val callbacks = Array.ofDim[OperationCallback](blockIds.length)
    for (i <- blockIds.indices) {
      val blockId = SparkBlockId.apply(blockIds(i)).asInstanceOf[SparkShuffleBlockId]
      ucxBlockIds(i) = UcxShuffleBockId(blockId.shuffleId, mapId2PartitionId(blockId.mapId), blockId.reduceId)
      callbacks(i) = (result: OperationResult) => {
        logInfo(s"Received ${ucxBlockIds(i)} " +
          s"in ${result.getStats.get.getElapsedTimeNs} ns")
        val memBlock = result.getData
        val buffer = UnsafeUtils.getByteBufferView(memBlock.address, memBlock.size.toInt)
        listener.onBlockFetchSuccess(blockIds(i), new NioManagedBuffer(buffer) {
          override def release: ManagedBuffer = {
            memBlock.close()
            this
          }
        })
      }
    }
    val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds, resultBufferAllocator, callbacks)
    transport.progress()
  }

  override def close(): Unit = {

  }
}
