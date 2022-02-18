/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap
import scala.util.Random

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.utils.SerializationUtils
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.unsafe.Platform


class UcxFailureOperationResult(errorMsg: String) extends OperationResult {
  override def getStatus: OperationStatus.Value = OperationStatus.FAILURE

  override def getError: TransportError = new TransportError(errorMsg)

  override def getStats: Option[OperationStats] = None

  override def getData: MemoryBlock = null
}

class UcxAmDataMemoryBlock(ucpAmData: UcpAmData)
  extends MemoryBlock(ucpAmData.getDataAddress, ucpAmData.getLength, true) with Logging {

  override def close(): Unit = {
    ucpAmData.close()
  }
}

/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
class UcxWorkerWrapper(val worker: UcpWorker, val transport: UcxShuffleTransport)
  extends Closeable with Logging {

  private final val connections =  new TrieMap[transport.ExecutorId, UcpEndpoint]
  private val requestData = new TrieMap[Int, (OperationCallback, UcxRequest, transport.BufferAllocator)]
  private val tag = new AtomicInteger(Random.nextInt())

  // Receive block data handler
  worker.setAmRecvHandler(1,
    (headerAddress: Long, headerSize: Long, ucpAmData: UcpAmData, _: UcpEndpoint) => {
      val headerBuffer = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
      val i = headerBuffer.getInt(0)
      val data = requestData.remove(i)

      if (data.isEmpty) {
        throw new UcxException(s"No data for tag $i.")
      }

      val (callback, request, allocator) = data.get
      val stats = request.getStats.get.asInstanceOf[UcxStats]
      stats.receiveSize = ucpAmData.getLength
      if (ucpAmData.isDataValid) {
        if (callback != null) {
          request.completed = true
          stats.endTime = System.nanoTime()
          logDebug(s"Received amData: $ucpAmData for tag $i " +
            s"in ${stats.getElapsedTimeNs} ns")
          callback.onComplete(new OperationResult {
            override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

            override def getError: TransportError = null

            override def getStats: Option[OperationStats] = Some(stats)

            override def getData: MemoryBlock = new UcxAmDataMemoryBlock(ucpAmData)
          })
          UcsConstants.STATUS.UCS_INPROGRESS
        } else {
          UcsConstants.STATUS.UCS_OK
        }
      } else {
        val mem = allocator(ucpAmData.getLength)
        stats.amHandleTime = System.nanoTime()
        request.setRequest(worker.recvAmDataNonBlocking(ucpAmData.getDataHandle, mem.address, ucpAmData.getLength,
          new UcxCallback() {
          override def onSuccess(r: UcpRequest): Unit = {
            request.completed = true
            stats.endTime = System.nanoTime()
            logDebug(s"Received rndv data of size: ${mem.size} for tag $i in " +
              s"${stats.getElapsedTimeNs} ns " +
              s"time from amHandle: ${System.nanoTime() - stats.amHandleTime} ns")
            callback.onComplete(new OperationResult {
              override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

              override def getError: TransportError = null

              override def getStats: Option[OperationStats] = Some(stats)

              override def getData: MemoryBlock = mem
            })
          }
        }, UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST))
        UcsConstants.STATUS.UCS_OK
      }
    }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  override def close(): Unit = {
    val closeRequests = connections.map {
      case (_, endpoint) => endpoint.closeNonBlockingForce()
    }
    while (!closeRequests.forall(_.isCompleted)) {
      progress()
    }
    connections.clear()
    worker.close()
  }

  /**
   * Blocking progress single request until it's not completed.
   */
  def waitRequest(request: UcpRequest): Unit = {
    while (!request.isCompleted) {
      progress()
    }
  }


  /**
   * The only place for worker progress
   */
  def progress(): Int = {
    worker.progress()
  }

  /**
   * Establish connections to known instances.
   */
  def preconnect(): Unit = {
    transport.executorAddresses.keys.foreach(getConnection)
  }

  def getConnection(executorId: transport.ExecutorId): UcpEndpoint = {

    val startTime = System.currentTimeMillis()
    while (!transport.executorAddresses.contains(executorId)) {
      if  (System.currentTimeMillis() - startTime >
        transport.ucxShuffleConf.getSparkConf.getTimeAsMs("spark.network.timeout", "100")) {
        throw new UcxException(s"Don't get a worker address for $executorId")
      }
    }

    connections.getOrElseUpdate(executorId,  {
      val endpointParams = new UcpEndpointParams().setPeerErrorHandlingMode()
        .setErrorHandler(new UcpEndpointErrorHandler() {
          override def onError(ep: UcpEndpoint, status: Int, errorMsg: String): Unit = {
            logError(s"Endpoint to $executorId got an error: $errorMsg")
            connections.remove(executorId)
          }
        }).setName(s"Endpoint to $executorId")
      val address = transport.executorAddresses(executorId)
      if (transport.ucxShuffleConf.useSockAddr) {
        logInfo(s"Worker $worker connecting to Executor($executorId, " +
          s"${SerializationUtils.deserializeInetAddress(address)}")
        endpointParams.setSocketAddress(SerializationUtils.deserializeInetAddress(address)).sendClientId()
      } else {
        endpointParams.setUcpAddress(address)
      }
      worker.newEndpoint(endpointParams)
    })
  }

  def fetchBlocksByBlockIds(executorId: transport.ExecutorId, blockIds: Seq[BlockId],
                            resultBufferAllocator: transport.BufferAllocator,
                            callbacks: Seq[OperationCallback]): Seq[Request] = {
    val startTime = System.nanoTime()
    val ep = getConnection(executorId)
    val t = tag.getAndAdd(blockIds.length)

    val buffer = Platform.allocateDirectBuffer(4 + blockIds.map(_.serializedSize).sum)
    buffer.putInt(t)
    blockIds.foreach(b => b.serialize(buffer))

    val requests = new Array[UcxRequest](blockIds.size)
    for (i <- blockIds.indices) {
      val stats = new UcxStats()
      requests(i) = new UcxRequest(null, stats)
      requestData.put(t + i, (callbacks(i), requests(i), resultBufferAllocator))
    }

    val address = UnsafeUtils.getAdress(buffer)
    val dataAddress = address + 4

    ep.sendAmNonBlocking(0, address, 4, dataAddress, buffer.capacity() - 4,
      UcpConstants.UCP_AM_SEND_FLAG_REPLY | UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback() {
       override def onSuccess(request: UcpRequest): Unit = {
         buffer.clear()
         logInfo(s"Sent message to $executorId to fetch ${blockIds.length} blocks on tag $t " +
           s"in ${System.nanoTime() - startTime} ns")
       }
     }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

    requests
  }

}
