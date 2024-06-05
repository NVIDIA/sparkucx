/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.nio.ByteBuffer
import java.util.{HashSet, HashMap}
import java.util.concurrent.locks.StampedLock
import org.openucx.jucx.ucp.UcpRequest

/**
 * Class that represents some block in memory with it's address, size.
 *
 * @param isHostMemory host or GPU memory
 */
case class MemoryBlock(address: Long, size: Long, isHostMemory: Boolean = true) extends AutoCloseable {
  /**
   * Important to call this method, to return memory to pool, or close resources
   */
  override def close(): Unit = {}
}

/**
 * Base class to indicate some blockId. It should be hashable and could be constructed on both ends.
 * E.g. ShuffleBlockId(shuffleId: Int, mapId: Long, reduceId: Int)
 */
trait BlockId {
  def serializedSize: Int
  def serialize(byteBuffer: ByteBuffer): Unit
}

private[ucx] sealed trait BlockLock {
  // Private transport lock to know when there are outstanding operations to block memory.
  private[ucx] lazy val lock = new StampedLock().asReadWriteLock()
}

/**
 * Some block in memory, that transport registers and that would requested on a remote side.
 */
trait Block extends BlockLock {
  def getSize: Long

  // This method for future use with a device buffers.
  def getMemoryBlock: MemoryBlock = ???

  // Get block from a file into byte buffer backed bunce buffer
  def getBlock(byteBuffer: ByteBuffer): Unit
}

object OperationStatus extends Enumeration {
  val SUCCESS, CANCELED, FAILURE = Value
}

/**
 * Operation statistic, like completionTime, transport used, protocol used, etc.
 */
trait OperationStats {
  /**
   * Time it took from operation submit to callback call.
   * This depends on [[ ShuffleTransport.progress() ]] calls,
   * and does not indicate actual data transfer time.
   */
  def getElapsedTimeNs: Long

  /**
   * Indicates number of valid bytes in receive memory when using
   * [[ ShuffleTransport.fetchBlocksByBlockIds()]]
   */
  def recvSize: Long
}

class TransportError(errorMsg: String) extends Exception(errorMsg)

trait OperationResult {
  def getStatus: OperationStatus.Value
  def getError: TransportError
  def getStats: Option[OperationStats]
  def getData: MemoryBlock
}

/**
 * Request object that returns by [[ ShuffleTransport.fetchBlocksByBlockIds() ]] routine.
 */
trait Request {
  def isCompleted: Boolean
  def getStats: Option[OperationStats]
}

/**
 * Async operation callbacks
 */
trait OperationCallback {
  def onComplete(result: OperationResult): Unit
  def onError(result: OperationResult): Unit = ???
  def onData(buf: ByteBuffer): Unit = ???
}

/**
 * Transport flow example:
 * val transport = new UcxShuffleTransport()
 * transport.init()
 *
 * Mapper/writer:
 * transport.register(blockId, block)
 *
 * Reducer:
 * transport.fetchBlockByBlockId(blockId, resultBounceBuffer)
 * transport.progress()
 *
 * transport.unregister(blockId)
 * transport.close()
 */
trait ShuffleTransport {
  type ExecutorId = Long
  type BufferAllocator = Long => MemoryBlock
  /**
   * Initialize transport resources. This function should get called after ensuring that SparkConf
   * has the correct configurations since it will use the spark configuration to configure itself.
   *
   * @return worker address of current process, to use in [[ addExecutor()]]
   */
  def init(): ByteBuffer

  /**
   * Close all transport resources
   */
  def close(): Unit

  /**
   * Add executor's worker address. For standalone testing purpose and for implementations that makes
   * connection establishment outside of UcxShuffleManager.
   */
  def addExecutor(executorId: ExecutorId, workerAddress: ByteBuffer): Unit

  /**
   * Remove executor from communications.
   */
  def removeExecutor(executorId: ExecutorId): Unit

  /**
   * Registers blocks using blockId on SERVER side.
   */
  def register(blockId: BlockId, block: Block): Unit

  /**
   * Change location of underlying blockId in memory
   */
  def mutate(blockId: BlockId, newBlock: Block, callback: OperationCallback): Unit

  /**
   * Indicate that this blockId is not needed any more by an application.
   * Note: this is a blocking call. On return it's safe to free blocks memory.
   */
  def unregister(blockId: BlockId): Unit

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(executorId: ExecutorId, blockIds: Seq[BlockId],
                            resultBufferAllocator: BufferAllocator,
                            callbacks: Seq[OperationCallback]): Seq[Request]

  /**
   * Progress outstanding operations. This routine is blocking (though may poll for event).
   * It's required to call this routine within same thread that submitted [[ fetchBlocksByBlockIds ]].
   *
   * Return from this method guarantees that at least some operation was progressed.
   * But not guaranteed that at least one [[ fetchBlocksByBlockIds ]] completed!
   */
  def progress(): Unit

}

class UcxRequest(private var request: UcpRequest, stats: OperationStats)
  extends Request {

  private[ucx] var completed = false

  override def isCompleted: Boolean = completed || ((request != null) && request.isCompleted)

  override def getStats: Option[OperationStats] = Some(stats)

  override def toString(): String = {
    s"UcxRequest(isCompleted=$isCompleted size=${stats.recvSize} cost=${stats.getElapsedTimeNs}ns)"
  }

  private[ucx] def setRequest(request: UcpRequest): Unit = {
    this.request = request
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

class UcxFetchState(val callbacks: Seq[OperationCallback],
                    val request: UcxRequest,
                    val timestamp: Long,
                    val recvSet: HashSet[Int] = new HashSet[Int]) {
  override def toString(): String = {
    s"UcxFetchState(chunks=${callbacks.size}, $request, start=$timestamp, received=${recvSet.size})"
  }
}

class UcxStreamState(val callback: OperationCallback,
                     val request: UcxRequest,
                     val timestamp: Long,
                     var remaining: Long,
                     val recvMap: HashMap[Long, MemoryBlock] = new HashMap[Long, MemoryBlock]) {
  override def toString(): String = {
    s"UcxStreamState($request, start=$timestamp, remaining=$remaining, received=${recvMap.size})"
  }
}

class UcxSliceState(val callback: OperationCallback,
                    val request: UcxRequest,
                    val timestamp: Long,
                    val mem: MemoryBlock,
                    var remaining: Long,
                    var offset: Long = 0L,
                    val recvSet: HashSet[Long] = new HashSet[Long]) {
  override def toString(): String = {
    s"UcxStreamState($request, start=$timestamp, remaining=$remaining, received=${recvSet.size})"
  }
}

class UcxSucceedOperationResult(mem: MemoryBlock, stats: OperationStats)
  extends OperationResult {
  override def getStatus: OperationStatus.Value = OperationStatus.SUCCESS

  override def getError: TransportError = null

  override def getStats: Option[OperationStats] = Option(stats)

  override def getData: MemoryBlock = mem
}