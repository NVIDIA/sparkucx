package org.apache.spark.shuffle.compat.spark_3_0

import org.openucx.jucx.UcxUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, BlockStoreClient}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBlockId, ExternalUcxClientTransport, UcxFetchCallBack, UcxDownloadCallBack}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class ExternalUcxShuffleClient(val transport: ExternalUcxClientTransport) extends BlockStoreClient{

  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    if (downloadFileManager == null) {
      val ucxBlockIds = Array.ofDim[UcxShuffleBlockId](blockIds.length)
      val callbacks = Array.ofDim[OperationCallback](blockIds.length)
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        ucxBlockIds(i) = UcxShuffleBlockId(blockId.shuffleId, blockId.mapId,
                                           blockId.reduceId)
        callbacks(i) = new UcxFetchCallBack(blockIds(i), listener)
      }
      val maxBlocksPerRequest = transport.getMaxBlocksPerRequest
      for (i <- 0 until blockIds.length by maxBlocksPerRequest) {
        val j = i + maxBlocksPerRequest
        transport.fetchBlocksByBlockIds(host, execId.toInt,
                                        ucxBlockIds.slice(i, j),
                                        callbacks.slice(i, j))
      }
    } else {
      for (i <- blockIds.indices) {
        val blockId = SparkBlockId.apply(blockIds(i))
                                  .asInstanceOf[SparkShuffleBlockId]
        val ucxBid = UcxShuffleBlockId(blockId.shuffleId, blockId.mapId,
                                       blockId.reduceId)
        val callback = new UcxDownloadCallBack(blockIds(i), listener,
                                               downloadFileManager,
                                               transport.sparkTransportConf)
        transport.fetchBlockByStream(host, execId.toInt, ucxBid, callback)
      }
    }
  }

  override def close(): Unit = {

  }
}