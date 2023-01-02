/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}

import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.shuffle.utils.UnsafeUtils

class FileBackedMemoryBlock(baseAddress: Long, baseSize: Long, address: Long, size: Long)
  extends MemoryBlock(address, size) {
  override def close(): Unit = {
    UnsafeUtils.munmap(baseAddress, baseSize)
  }
}

/**
 * Mapper entry point for UcxShuffle plugin. Performs memory registration
 * of data and index files and publish addresses to driver metadata buffer.
 */
abstract class CommonUcxShuffleBlockResolver(ucxShuffleManager: CommonUcxShuffleManager)
  extends IndexShuffleBlockResolver(ucxShuffleManager.conf) {

  private val openFds = new ConcurrentHashMap[ShuffleId, ConcurrentLinkedQueue[RandomAccessFile]]()

  /**
   * Mapper commit protocol extension. Register index and data files and publish all needed
   * metadata to driver.
   */
  def writeIndexFileAndCommitCommon(shuffleId: ShuffleId, mapId: Int,
                                    lengths: Array[Long], dataBackFile: RandomAccessFile): Unit = {
    openFds.computeIfAbsent(shuffleId, (_: ShuffleId) => new ConcurrentLinkedQueue[RandomAccessFile]())
    openFds.get(shuffleId).add(dataBackFile)
    var offset = 0L
    val channel = dataBackFile.getChannel
    for ((blockLength, reduceId) <- lengths.zipWithIndex) {
      if (blockLength > 0) {
        val blockId = UcxShuffleBlockId(shuffleId, mapId ,reduceId)
        val block = new Block {
          private val fileOffset = offset

          override def getBlock(byteBuffer: ByteBuffer): Unit = {
            channel.read(byteBuffer, fileOffset)
          }

          override def getSize: Long = blockLength
        }
        ucxShuffleManager.ucxTransport.register(blockId, block)
        offset += blockLength
      }
    }
  }

  def removeShuffle(shuffleId: Int): Unit = {
    val fds = openFds.remove(shuffleId)
    if (fds != null) {
      fds.forEach(f => f.close())
    }
    if (ucxShuffleManager.ucxTransport != null) {
      ucxShuffleManager.ucxTransport.unregisterShuffle(shuffleId)
    }
  }

  override def stop(): Unit = {
    if (ucxShuffleManager.ucxTransport != null) {
      ucxShuffleManager.ucxTransport.unregisterAllBlocks()
    }
  }
}
