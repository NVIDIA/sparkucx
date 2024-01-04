package org.apache.spark.shuffle.compat.spark_2_4

import org.openucx.jucx.UcxUtils
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, ShuffleClient}
import org.apache.spark.shuffle.ucx.{OperationCallback, OperationResult, UcxShuffleBockId, UcxShuffleTransport}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.storage.{BlockId => SparkBlockId, ShuffleBlockId => SparkShuffleBlockId}

class UcxShuffleClient(val transport: UcxShuffleTransport) extends ShuffleClient{
  override def fetchBlocks(host: String, port: Int, execId: String, blockIds: Array[String],
                           listener: BlockFetchingListener,
                           downloadFileManager: DownloadFileManager): Unit = {
    val ucxBlockIds = Array.ofDim[UcxShuffleBockId](blockIds.length)
    val callbacks = Array.ofDim[OperationCallback](blockIds.length)
    for (i <- blockIds.indices) {
      val blockId = SparkBlockId.apply(blockIds(i)).asInstanceOf[SparkShuffleBlockId]
      ucxBlockIds(i) = UcxShuffleBockId(blockId.shuffleId, blockId.mapId, blockId.reduceId)
      callbacks(i) = (result: OperationResult) => {
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
    val maxBlocksPerRequest= transport.maxBlocksPerRequest
    val resultBufferAllocator = (size: Long) => transport.hostBounceBufferMemoryPool.get(size)
    for (i <- 0 until blockIds.length by maxBlocksPerRequest) {
      val j = i + maxBlocksPerRequest
      transport.fetchBlocksByBlockIds(execId.toLong, ucxBlockIds.slice(i, j),
                                      resultBufferAllocator,
                                      callbacks.slice(i, j))
    }
  }

  override def close(): Unit = {

  }
}
