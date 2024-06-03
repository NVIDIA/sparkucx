package org.apache.spark.shuffle.compat.spark_2_4

import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ShuffleClient}
import org.apache.spark.shuffle.ucx.{UcxFetchCallBack, UcxDownloadCallBack, UcxShuffleBockId, UcxShuffleTransport}
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class UcxShuffleClient(val transport: UcxShuffleTransport) extends ShuffleClient{
  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    if (downloadFileManager == null) {
      val ucxBlockIds = Array.ofDim[UcxShuffleBockId](blockIds.length)
      val callbacks = Array.ofDim[UcxFetchCallBack](blockIds.length)
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        ucxBlockIds(i) = UcxShuffleBockId(blockId.shuffleId, blockId.mapId,
                                          blockId.reduceId)
        callbacks(i) = new UcxFetchCallBack(blockIds(i), listener)
      }
      val maxBlocksPerRequest= transport.maxBlocksPerRequest
      val resultBufferAllocator = transport.hostBounceBufferMemoryPool.get _
      for (i <- 0 until blockIds.length by maxBlocksPerRequest) {
        val j = i + maxBlocksPerRequest
        transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds.slice(i, j),
                                        resultBufferAllocator,
                                        callbacks.slice(i, j))
      }
    } else {
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        val ucxBlockId = UcxShuffleBockId(blockId.shuffleId, blockId.mapId,
                                          blockId.reduceId)
        val callback = new UcxDownloadCallBack(blockIds(i), listener,
                                               downloadFileManager,
                                               transport.sparkTransportConf)
        transport.fetchBlockByStream(execId.toLong, ucxBlockId, callback)
      }
    }
  }

  override def close(): Unit = {

  }
}
