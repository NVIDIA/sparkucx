/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.io.Closeable
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.concurrent.{ConcurrentLinkedQueue, ConcurrentHashMap, CountDownLatch, Future, FutureTask}
import scala.collection.mutable
import scala.collection.JavaConverters._
import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.shuffle.ucx.memory.UcxLinkedMemBlock
import org.apache.spark.shuffle.utils.{UnsafeUtils, UcxLogging}
import java.net.InetSocketAddress

class ExternalUcxEndpoint(val ucpEp: UcpEndpoint, var closed: Boolean) {}
/**
 * Worker per thread wrapper, that maintains connection and progress logic.
 */
case class ExternalUcxServerWorker(val worker: UcpWorker,
                                   transport: ExternalUcxServerTransport,
                                   workerId: UcxWorkerId,
                                   port: Int)
  extends Closeable with UcxLogging {
  private[this] val memPool = transport.hostBounceBufferMemoryPool(workerId.workerId)
  private[this] val maxReplySize = transport.getMaxReplySize()
  private[this] val shuffleClients = new ConcurrentHashMap[UcxWorkerId, ExternalUcxEndpoint]
  private[ucx] val executor = new UcxWorkerThread(
    worker, transport.ucxShuffleConf.useWakeup)

  private val emptyCallback = () => {}
  private val endpoints = mutable.HashMap.empty[UcpEndpoint, () => Unit]
  private val listener = worker.newListener(
    new UcpListenerParams().setSockAddr(new InetSocketAddress("0.0.0.0", port))
    .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
      val clientAddress = ucpConnectionRequest.getClientAddress()
      try {
        val ep = worker.newEndpoint(
          new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
            .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
            .setName(s"Endpoint to $clientAddress"))
        endpoints.getOrElseUpdate(ep, emptyCallback)
      } catch {
        case e: UcxException => logError(s"Accept $clientAddress fail: $e")
      }
    }))

  private val listenerAddress = listener.getAddress
  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
        logInfo(s"Connection closed on ep: $ucpEndpoint")
      } else {
        logWarning(s"Ep $ucpEndpoint got an error: $errorString")
      }
      endpoints.remove(ucpEndpoint).foreach(_())
      ucpEndpoint.close()
    }
  }

  // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
  worker.setAmRecvHandler(ExternalAmId.FETCH_BLOCK,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val shuffleClient = UcxWorkerId.deserialize(header)
    val replyTag = header.getInt
    val exeId = header.getInt
    val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                               amData.getLength.toInt)
    val BlockNum = buffer.remaining() / UcxShuffleBlockId.serializedSize
    val blockIds = (0 until BlockNum).map(
      _ => UcxShuffleBlockId.deserialize(buffer))
    logTrace(s"${workerId.workerId} Recv fetch from $shuffleClient tag $replyTag.")
    transport.handleFetchBlockRequest(this, shuffleClient, exeId, replyTag, blockIds)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  // Main RPC thread. Submit each RPC request to separate thread and send stream back from separate worker.
  worker.setAmRecvHandler(ExternalAmId.FETCH_STREAM,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val shuffleClient = UcxWorkerId.deserialize(header)
    val replyTag = header.getInt
    val exeId = header.getInt
    val blockId = UcxShuffleBlockId.deserialize(header)
    logTrace(s"${workerId.workerId} Recv stream from $shuffleClient tag $replyTag.")
    transport.handleFetchBlockStream(this, shuffleClient, exeId, replyTag, blockId)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )

  // AM to get worker address for client worker and connect server workers to it
  worker.setAmRecvHandler(ExternalAmId.CONNECT,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val shuffleClient = UcxWorkerId.deserialize(header)
    val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                                      amData.getLength.toInt)
    val copiedAddress = ByteBuffer.allocateDirect(workerAddress.remaining)
    copiedAddress.put(workerAddress)
    connected(shuffleClient, copiedAddress)
    endpoints.put(ep, () => doDisconnect(shuffleClient))
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
  // Main RPC thread. reply with ucpAddress.

  // AM to get worker address for client worker and connect server workers to it
  worker.setAmRecvHandler(ExternalAmId.ADDRESS,
    (headerAddress: Long, headerSize: Long, amData: UcpAmData, ep: UcpEndpoint) => {
    handleAddress(ep)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )
  // Main RPC thread. reply with ucpAddress.

  def start(): Unit = {
    executor.start()
  }

  override def close(): Unit = {
    if (!shuffleClients.isEmpty) {
      logInfo(s"$workerId closing ${shuffleClients.size} clients")
      shuffleClients.values.asScala.map(
        _.ucpEp.closeNonBlockingFlush()).foreach(req =>
          while (!req.isCompleted){
            worker.progress()
          }
        )
    }
    if (!endpoints.isEmpty) {
      logInfo(s"$workerId closing ${endpoints.size} eps")
      endpoints.keys.map(
        _.closeNonBlockingForce()).foreach(req =>
          while (!req.isCompleted){
            worker.progress()
          }
        )
    }
    listener.close()
  }

  def closing(): Future[Unit.type] = {
    val cleanTask = new FutureTask(new Runnable {
      override def run() = close()
    }, Unit)
    executor.close(cleanTask)
    cleanTask
  }

  def getPort(): Int = {
    listenerAddress.getPort()
  }

  def getAddress(): InetSocketAddress = {
    listenerAddress
  }

  @`inline`
  private def doDisconnect(shuffleClient: UcxWorkerId): Unit = {
    try {
      Option(shuffleClients.remove(shuffleClient)).foreach(ep => {
        ep.ucpEp.closeNonBlockingFlush()
        ep.closed = true
        logDebug(s"Disconnect $shuffleClient")
      })
    } catch {
      case e: Throwable => logWarning(s"Disconnect $shuffleClient: $e")
    }
  }

  @`inline`
  def isEpClosed(ep: UcpEndpoint): Boolean = {
    ep.getNativeId() == null
  }

  @`inline`
  def disconnect(workerIds: Seq[UcxWorkerId]): Unit = {
    executor.post(() => workerIds.foreach(doDisconnect(_)))
  }

  @`inline`
  def connected(shuffleClient: UcxWorkerId, workerAddress: ByteBuffer): Unit = {
    logDebug(s"$workerId connecting back to $shuffleClient by worker address")
    try {
      shuffleClients.computeIfAbsent(shuffleClient, _ => {
        val ucpEp = worker.newEndpoint(new UcpEndpointParams()
          .setErrorHandler(new UcpEndpointErrorHandler {
            override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
              logInfo(s"Connection to $shuffleClient closed: $errorString")
              shuffleClients.remove(shuffleClient)
              ucpEndpoint.close()
            }
          })
          .setName(s"Server to $shuffleClient")
          .setUcpAddress(workerAddress))
        new ExternalUcxEndpoint(ucpEp, false)
      })
    } catch {
      case e: UcxException => logWarning(s"Connection to $shuffleClient failed: $e")
    }
  }

  def awaitConnection(shuffleClient: UcxWorkerId): ExternalUcxEndpoint = {
    shuffleClients.getOrDefault(shuffleClient, {
      // wait until connected finished
      val startTime = System.currentTimeMillis()
      while (!shuffleClients.containsKey(shuffleClient)) {
        if  (System.currentTimeMillis() - startTime > 10000) {
          throw new UcxException(s"Don't get a worker address for $shuffleClient")
        }
        Thread.`yield`
      }
      shuffleClients.get(shuffleClient)
    })
  }

  def handleAddress(ep: UcpEndpoint) = {
    val msg = transport.getServerPortsBuffer()
    val header = UnsafeUtils.getAdress(msg)
    ep.sendAmNonBlocking(
      ExternalAmId.REPLY_ADDRESS, header, msg.remaining(), header, 0,
      UcpConstants.UCP_AM_SEND_FLAG_EAGER, new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logTrace(s"$workerId sent to REPLY_ADDRESS to $ep")
          msg.clear()
        }

        override def onError(ucsStatus: Int, errorMsg: String): Unit = {
          logWarning(s"$workerId sent to REPLY_ADDRESS to $ep: $errorMsg")
          msg.clear()
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
    logTrace(s"$workerId sending to REPLY_ADDRESS $msg mem $header")
  }

  def handleFetchBlockRequest(
    clientWorker: UcxWorkerId, replyTag: Int,
    blockInfos: Seq[(FileChannel, Long, Long)]): Unit = {
    val blockSize = blockInfos.map(x => x._3).sum
    if (blockSize <= maxReplySize) {
      handleFetchBlockChunks(clientWorker, replyTag, blockInfos, blockSize)
      return
    }
    // The size of last block could > maxBytesInFlight / 5 in spark.
    val lastBlock = blockInfos.last
    if (blockInfos.size > 1) {
      val chunks = blockInfos.slice(0, blockInfos.size - 1)
      val chunksSize = blockSize - lastBlock._3
      handleFetchBlockChunks(clientWorker, replyTag, chunks, chunksSize)
    }
    handleFetchBlockStream(clientWorker, replyTag, lastBlock,
                           ExternalAmId.REPLY_SLICE)
  }

  def handleFetchBlockChunks(
    clientWorker: UcxWorkerId, replyTag: Int,
    blockInfos: Seq[(FileChannel, Long, Long)], blockSize: Long): Unit = {
    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blockInfos.length
    val msgSize = tagAndSizes + blockSize
    val resultMemory = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
    assert(resultMemory != null)
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address, msgSize)

    resultBuffer.putInt(replyTag)
    val blocksRange = 0 until blockInfos.length
    for (i <- blocksRange) {
      resultBuffer.putInt(blockInfos(i)._3.toInt)
    }

    for (i <- blocksRange) {
      val (blockCh, blockOffset, blockSize) = blockInfos(i)
      resultBuffer.limit(resultBuffer.position() + blockSize.toInt)
      blockCh.read(resultBuffer, blockOffset)
    }

    val ep = awaitConnection(clientWorker)
    executor.post(new Runnable {
      override def run(): Unit = {
        if (ep.closed) {
          resultMemory.close()
          return
        }

        val startTime = System.nanoTime()
        val req = ep.ucpEp.sendAmNonBlocking(ExternalAmId.REPLY_BLOCK,
          resultMemory.address, tagAndSizes, resultMemory.address + tagAndSizes,
          msgSize - tagAndSizes, 0, new UcxCallback {
            override def onSuccess(request: UcpRequest): Unit = {
              resultMemory.close()
              logTrace(s"${workerId.workerId} Sent to ${clientWorker} ${blockInfos.length} blocks of size: " +
                s"${msgSize} tag $replyTag in ${System.nanoTime() - startTime} ns.")
            }

            override def onError(ucsStatus: Int, errorMsg: String): Unit = {
              resultMemory.close()
              logError(s"${workerId.workerId} Failed to reply fetch $clientWorker tag $replyTag $errorMsg.")
            }
          }, new UcpRequestParams().setMemoryType(
            UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
        .setMemoryHandle(resultMemory.memory))
      }
    })
    logTrace(s"${workerId.workerId} Sending to $clientWorker tag $replyTag mem $resultMemory size $msgSize")
  }

  def handleFetchBlockStream(clientWorker: UcxWorkerId, replyTag: Int,
                             blockInfo: (FileChannel, Long, Long),
                             amId: Int = ExternalAmId.REPLY_STREAM): Unit = {
    // tag: Int + unsent replies: Int + total length: Long + offset now: Long
    val headerSize = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE +
                     UnsafeUtils.LONG_SIZE + UnsafeUtils.LONG_SIZE
    val (blockCh, blockOffset, blockSize) = blockInfo
    val blockSlice = (0L until blockSize by maxReplySize).toArray
    // make sure the last one is not too small
    if (blockSlice.size >= 2) {
      val mid = (blockSlice(blockSlice.size - 2) + blockSize) / 2
      blockSlice(blockSlice.size - 1) = mid
    }

    def send(workerWrapper: ExternalUcxServerWorker, currentId: Int): Unit = try {
      val hashNext = (currentId + 1 != blockSlice.size)
      val nextOffset = if (hashNext) blockSlice(currentId + 1) else blockSize
      val currentOffset = blockSlice(currentId)
      val currentSize = nextOffset - currentOffset
      val unsent = blockSlice.length - currentId - 1
      val msgSize = headerSize + currentSize.toInt
      val mem = memPool.get(msgSize).asInstanceOf[UcxLinkedMemBlock]
      val buffer = mem.toByteBuffer()

      buffer.limit(msgSize)
      buffer.putInt(replyTag)
      buffer.putInt(unsent)
      buffer.putLong(blockSize)
      buffer.putLong(currentOffset)
      blockCh.read(buffer, blockOffset + currentOffset)

      val ep = workerWrapper.awaitConnection(clientWorker)
      workerWrapper.executor.post(new Runnable {
        override def run(): Unit = {
          if (ep.closed) {
            mem.close()
            return
          }

          val startTime = System.nanoTime()
          val req = ep.ucpEp.sendAmNonBlocking(amId, mem.address, headerSize,
            mem.address + headerSize, currentSize, 
            UcpConstants.UCP_AM_SEND_FLAG_RNDV, new UcxCallback {
              override def onSuccess(request: UcpRequest): Unit = {
                mem.close()
                logTrace(s"${workerId.workerId} Sent to ${clientWorker} size $currentSize tag $replyTag seg " +
                  s"$currentId in ${System.nanoTime() - startTime} ns.")
              }
              override def onError(ucsStatus: Int, errorMsg: String): Unit = {
                mem.close()
                logError(s"${workerId.workerId} Failed to reply stream $clientWorker tag $replyTag $currentId $errorMsg.")
              }
            }, new UcpRequestParams()
              .setMemoryType(UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)
              .setMemoryHandle(mem.memory))
        }
      })
      logTrace(s"${workerId.workerId} Sending to $clientWorker tag $replyTag $currentId mem $mem size $msgSize.")
      if (hashNext) {
        transport.submit(() => send(this, currentId + 1))
      }
    } catch {
      case ex: Throwable =>
        logError(s"${workerId.workerId} Failed to reply stream $clientWorker tag $replyTag $currentId $ex.")
    }

    send(this, 0)
  }
}