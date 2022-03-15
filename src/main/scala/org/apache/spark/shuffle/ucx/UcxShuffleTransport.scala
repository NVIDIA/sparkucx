/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.parallel.ForkJoinTaskSupport

import org.openucx.jucx.ucp._
import org.openucx.jucx.ucs.UcsConstants
import org.openucx.jucx.ucs.UcsConstants.MEMORY_TYPE
import org.openucx.jucx.{UcxCallback, UcxException, UcxUtils}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.memory.UcxHostBounceBuffersPool
import org.apache.spark.shuffle.ucx.rpc.GlobalWorkerRpcThread
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.ThreadUtils

class UcxRequest(private var request: UcpRequest, stats: OperationStats)
  extends Request {

  private[ucx] var completed = false

  override def isCompleted: Boolean = completed || ((request != null) && request.isCompleted)

  override def getStats: Option[OperationStats] = Some(stats)

  private[ucx] def setRequest(request: UcpRequest): Unit = {
    this.request = request
  }
}

class UcxRegisteredMemoryBlock(private[ucx] val memory: UcpMemory)
  extends MemoryBlock(memory.getAddress, memory.getLength,
    memory.getMemType == UcsConstants.MEMORY_TYPE.UCS_MEMORY_TYPE_HOST) {

  override def close(): Unit = {
    memory.close()
  }
}

class UcxStats extends OperationStats {
  private[ucx] val startTime = System.nanoTime()
  private[ucx] var amHandleTime = 0L
  private[ucx] var endTime: Long = 0L
  private[ucx] var receiveSize: Long = 0L

  /**
   * Time it took from operation submit to callback call.
   * This depends on [[ ShuffleTransport.progress() ]] calls,
   * and does not indicate actual data transfer time.
   */
  override def getElapsedTimeNs: Long = endTime - startTime

  /**
   * Indicates number of valid bytes in receive memory
   */
  override def recvSize: Long = receiveSize
}

case class UcxShuffleBockId(shuffleId: Int, mapId: Int, reduceId: Int) extends BlockId {
  override def serializedSize: Int = 12

  override def serialize(byteBuffer: ByteBuffer): Unit = {
    byteBuffer.putInt(shuffleId)
    byteBuffer.putInt(mapId)
    byteBuffer.putInt(reduceId)
  }
}

object UcxShuffleBockId {
  def deserialize(byteBuffer: ByteBuffer): UcxShuffleBockId = {
    val shuffleId = byteBuffer.getInt
    val mapId = byteBuffer.getInt
    val reduceId = byteBuffer.getInt
    UcxShuffleBockId(shuffleId, mapId, reduceId)
  }
}

class UcxShuffleTransport(var ucxShuffleConf: UcxShuffleConf = null, var executorId: Long = 0) extends ShuffleTransport
  with Logging {
  @volatile private var initialized: Boolean = false
  private[ucx] var ucxContext: UcpContext = _
  private var globalWorker: UcpWorker = _
  private var listener: UcpListener = _
  private val ucpWorkerParams = new UcpWorkerParams().setClientId(executorId).requestThreadSafety()
  val endpoints = mutable.Set.empty[UcpEndpoint]
  val executorAddresses = new TrieMap[ExecutorId, ByteBuffer]
  private var workerAddress: ByteBuffer = _
  private var allocatedWorkers: Array[UcxWorkerWrapper] = _
  private val registeredBlocks = new TrieMap[BlockId, Block]
  private var progressThread: Thread = _
  var hostBounceBufferMemoryPool: UcxHostBounceBuffersPool = _
  private val threadPool = ThreadUtils.newForkJoinPool("IO threads",
    ucxShuffleConf.numIoThreads)
  private val taskSupport = new ForkJoinTaskSupport(threadPool)

  private val errorHandler = new UcpEndpointErrorHandler {
    override def onError(ucpEndpoint: UcpEndpoint, errorCode: Int, errorString: String): Unit = {
      if (errorCode == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
        logWarning(s"Connection closed on ep: $ucpEndpoint")
      } else {
        logError(s"Ep $ucpEndpoint got an error: $errorString")
      }
      endpoints.remove(ucpEndpoint)
      ucpEndpoint.close()
    }
  }

  override def init(): ByteBuffer = this.synchronized {
    if (!initialized) {
      if (ucxShuffleConf == null) {
        ucxShuffleConf = new UcxShuffleConf(SparkEnv.get.conf)
      }

      val numEndpoints = ucxShuffleConf.numWorkers *
        ucxShuffleConf.getSparkConf.getInt("spark.executor.instances", 1)
      logInfo(s"Creating UCX context with an estimated number of endpoints: $numEndpoints")

      val params = new UcpParams().requestAmFeature().setMtWorkersShared(true).setEstimatedNumEps(numEndpoints)
        .requestAmFeature().setConfig("USE_MT_MUTEX", "yes")

      if (ucxShuffleConf.useWakeup) {
        params.requestWakeupFeature()
        ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
      }

      ucxContext = new UcpContext(params)
      globalWorker = ucxContext.newWorker(ucpWorkerParams)
      hostBounceBufferMemoryPool = new UcxHostBounceBuffersPool(ucxShuffleConf, ucxContext)

      workerAddress = if (ucxShuffleConf.useSockAddr) {
        val Array(host, port) = ucxShuffleConf.listenerAddress.split(":")
        listener = globalWorker.newListener(new UcpListenerParams().setSockAddr(
          new InetSocketAddress(host, port.toInt))
          .setConnectionHandler((ucpConnectionRequest: UcpConnectionRequest) => {
            endpoints.add(globalWorker.newEndpoint(new UcpEndpointParams().setConnectionRequest(ucpConnectionRequest)
              .setPeerErrorHandlingMode().setErrorHandler(errorHandler)
              .setName(s"Endpoint to ${ucpConnectionRequest.getClientId}")))
          }))
        logInfo(s"Started listener on ${listener.getAddress}")
        SerializationUtils.serializeInetAddress(listener.getAddress)
      } else {
        globalWorker.getAddress
      }

      progressThread = new GlobalWorkerRpcThread(globalWorker, this)
      progressThread.start()

      allocatedWorkers = new Array[UcxWorkerWrapper](ucxShuffleConf.numWorkers)
      logInfo(s"Allocating ${ucxShuffleConf.numWorkers} client workers")
      for (i <- 0 until ucxShuffleConf.numWorkers) {
        val worker = ucxContext.newWorker(ucpWorkerParams)
        allocatedWorkers(i) = new UcxWorkerWrapper(worker, this)
      }
      initialized = true
    }
    workerAddress
  }

  /**
   * Close all transport resources
   */
  override def close(): Unit = {
    if (initialized) {
      endpoints.foreach(_.closeNonBlockingForce())
      endpoints.clear()

      allocatedWorkers.foreach(_.close())

      if (listener != null) {
        listener.close()
        listener = null
      }

      if (progressThread != null) {
        progressThread.interrupt()
        progressThread.join(10)
      }

      if (globalWorker != null) {
        globalWorker.close()
        globalWorker = null
      }

      hostBounceBufferMemoryPool.close()

      if (ucxContext != null) {
        ucxContext.close()
        ucxContext = null
      }
      threadPool.shutdown()
    }
  }

  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  override def addExecutor(executorId: ExecutorId, workerAddress: ByteBuffer): Unit = {
    executorAddresses.put(executorId, workerAddress)
    allocatedWorkers.foreach(w => w.getConnection(executorId))
  }

  def addExecutors(executorIdsToAddress: Map[ExecutorId, SerializableDirectBuffer]): Unit = {
    executorIdsToAddress.foreach {
      case (executorId, address) => executorAddresses.put(executorId, address.value)
    }
  }

  def preConnect(): Unit = {
    allocatedWorkers.foreach(w => w.preconnect())
  }

  /**
   * Remove executor from communications.
   */
  override def removeExecutor(executorId: Long): Unit = executorAddresses.remove(executorId)

  /**
   * Registers blocks using blockId on SERVER side.
   */
  override def register(blockId: BlockId, block: Block): Unit = {
    registeredBlocks.put(blockId, block)
  }

  /**
   * Change location of underlying blockId in memory
   */
  override def mutate(blockId: BlockId, newBlock: Block, callback: OperationCallback): Unit = {
    unregister(blockId)
    register(blockId, newBlock)
    callback.onComplete(new OperationResult {
      override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

      override def getError: TransportError = null

      override def getStats: Option[OperationStats] = None

      override def getData: MemoryBlock = newBlock.getMemoryBlock
    })

  }

  /**
   * Indicate that this blockId is not needed any more by an application.
   * Note: this is a blocking call. On return it's safe to free blocks memory.
   */
  override def unregister(blockId: BlockId): Unit = {
    registeredBlocks.remove(blockId)
  }

  def unregisterShuffle(shuffleId: Int): Unit = {
    registeredBlocks.keysIterator.foreach {
      case bid@UcxShuffleBockId(sid, _, _) if sid == shuffleId => registeredBlocks.remove(bid)
    }
  }

  def unregisterAllBlocks(): Unit = {
    registeredBlocks.clear()
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  override def fetchBlocksByBlockIds(executorId: ExecutorId, blockIds: Seq[BlockId],
                                     resultBufferAllocator: BufferAllocator,
                                     callbacks: Seq[OperationCallback]): Seq[Request] = {
    allocatedWorkers((Thread.currentThread().getId % allocatedWorkers.length).toInt)
      .fetchBlocksByBlockIds(executorId, blockIds, resultBufferAllocator, callbacks)
  }

  def handleFetchBlockRequest(replyTag: Int, buffer: ByteBuffer, replyEp: UcpEndpoint): Unit = try {
    val blockIds = mutable.ArrayBuffer.empty[BlockId]

    // 1. Deserialize blockIds from header
    while (buffer.remaining() > 0) {
      val blockId = UcxShuffleBockId.deserialize(buffer)
      if (!registeredBlocks.contains(blockId)) {
        throw new UcxException(s"$blockId is not registered")
      }
      blockIds += blockId
    }

    val blocks = blockIds.map(bid => registeredBlocks(bid))
    val tagAndSizes = UnsafeUtils.INT_SIZE + UnsafeUtils.INT_SIZE * blockIds.length
    val resultMemory = hostBounceBufferMemoryPool.get(tagAndSizes + blocks.map(_.getSize).sum)
    val resultBuffer = UcxUtils.getByteBufferView(resultMemory.address,
      resultMemory.size)
    resultBuffer.putInt(replyTag)

    var offset = 0
    val localBuffers = blocks.zipWithIndex.map {
      case (block, i) =>
        resultBuffer.putInt(UnsafeUtils.INT_SIZE + i * UnsafeUtils.INT_SIZE, block.getSize.toInt)
        resultBuffer.position(tagAndSizes + offset)
        val localBuffer = resultBuffer.slice()
        offset += block.getSize.toInt
        localBuffer.limit(block.getSize.toInt)
        localBuffer
    }
    // Do parallel read of blocks
    val blocksCollection = blocks.indices.par
    blocksCollection.tasksupport = taskSupport
    for (i <- blocksCollection) {
      blocks(i).getBlock(localBuffers(i))
    }

    val startTime = System.nanoTime()
    replyEp.sendAmNonBlocking(1, resultMemory.address, tagAndSizes,
      resultMemory.address + tagAndSizes, resultMemory.size - tagAndSizes, 0, new UcxCallback {
        override def onSuccess(request: UcpRequest): Unit = {
          logTrace(s"Sent ${blockIds.length} blocks of size: ${resultMemory.size} " +
            s"to tag $replyTag in ${System.nanoTime() - startTime} ns.")
          hostBounceBufferMemoryPool.put(resultMemory)
        }
      }, MEMORY_TYPE.UCS_MEMORY_TYPE_HOST)

  } catch {
    case ex: Throwable => logError(s"Failed to read and send data: $ex")
  }

  /**
   * Progress outstanding operations. This routine is blocking (though may poll for event).
   * It's required to call this routine within same thread that submitted [[ fetchBlocksByBlockIds ]].
   *
   * Return from this method guarantees that at least some operation was progressed.
   * But not guaranteed that at least one [[ fetchBlocksByBlockIds ]] completed!
   */
  override def progress(): Unit = {
    allocatedWorkers((Thread.currentThread().getId % allocatedWorkers.length).toInt).progress()
  }

  def progressAll(): Unit = {
    allocatedWorkers.foreach(_.progress())
  }
}
