package org.apache.spark.shuffle.ucx

// import org.apache.spark.SparkEnv
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.shuffle.utils.{UcxLogging, UnsafeUtils}
import org.apache.spark.shuffle.ucx.memory.UcxLimitedMemPool
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}
import org.apache.spark.storage.BlockManagerId
import org.openucx.jucx.ucp._

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

class ExternalUcxClientTransport(clientConf: ExternalUcxClientConf, blockManagerId: BlockManagerId)
extends ExternalUcxTransport(clientConf) with UcxLogging {
  private[spark] val executorId = blockManagerId.executorId.toLong
  private[spark] val tcpServerPort = blockManagerId.port
  private[spark] val ucxServerPort = clientConf.ucxServerPort
  private[spark] val numWorkers = clientConf.numWorkers
  private[spark] val timeoutMs = clientConf.getSparkConf.getTimeAsSeconds(
    "spark.network.timeout", "120s") * 1000
  private[spark] val sparkTransportConf = SparkTransportConf.fromSparkConf(
    clientConf.getSparkConf, "ucx-shuffle", numWorkers)

  private[ucx] val ucxServers = new ConcurrentHashMap[String, InetSocketAddress]
  private[ucx] val localServerPortsDone = new CountDownLatch(1)
  private[ucx] var localServerPortsBuffer: ByteBuffer = _
  private[ucx] var localServerPorts: Seq[Int] = _

  private[ucx] lazy val currentWorkerId = new AtomicInteger()
  private[ucx] lazy val workerLocal = new ThreadLocal[ExternalUcxClientWorker]
  private[ucx] var allocatedWorker: Array[ExternalUcxClientWorker] = _

  private[ucx] val scheduledLatch = new CountDownLatch(1)

  private var maxBlocksPerRequest = 0

  override def estimateNumEps(): Int = numWorkers *
      clientConf.sparkConf.getInt("spark.executor.instances", 1)

  override def init(): ByteBuffer = {
    initContext()
    initMemoryPool()
  
    if (clientConf.useWakeup) {
      ucpWorkerParams.requestWakeupRX().requestWakeupTX().requestWakeupEdge()
    }

    logInfo(s"Allocating ${numWorkers} client workers")
    val appId = clientConf.sparkConf.getAppId
    allocatedWorker = new Array[ExternalUcxClientWorker](numWorkers)
    for (i <- 0 until numWorkers) {
      ucpWorkerParams.setClientId((executorId << 32) | i.toLong)
      val worker = ucxContext.newWorker(ucpWorkerParams)
      val workerId = new UcxWorkerId(appId, executorId.toInt, i)
      allocatedWorker(i) = new ExternalUcxClientWorker(worker, this, workerId)
    }

    logInfo(s"Launching ${numWorkers} client workers")
    allocatedWorker.foreach(_.start)

    val maxAmHeaderSize = allocatedWorker(0).worker.getMaxAmHeaderSize.toInt
    maxBlocksPerRequest = clientConf.maxBlocksPerRequest.min(
      (maxAmHeaderSize - UnsafeUtils.INT_SIZE) / UnsafeUtils.INT_SIZE)

    logInfo(s"Launching time-scheduled threads, period: $timeoutMs ms")
    initTaskPool(1)
    submit(() => progressTimeOut())

    logInfo(s"Connecting server.")

    val shuffleServer = new InetSocketAddress(blockManagerId.host, ucxServerPort)
    allocatedWorker(0).requestAddress(shuffleServer)
    localServerPortsDone.await()

    logInfo(s"Connected server $shuffleServer")

    initialized = true
    localServerPortsBuffer
  }

  override def close(): Unit = {
    if (initialized) {
      running = false

      scheduledLatch.countDown()

      if (allocatedWorker != null) {
        allocatedWorker.map(_.closing).foreach(_.get(5, TimeUnit.MILLISECONDS))
      }

      super.close()
    }
  }

  def getMaxBlocksPerRequest: Int = maxBlocksPerRequest

  // @inline
  // def selectWorker(): ExternalUcxClientWorker = {
  //   allocatedWorker(
  //     (currentWorkerId.incrementAndGet() % allocatedWorker.length).abs)
  // }

  @inline
  def selectWorker(): ExternalUcxClientWorker = {
    Option(workerLocal.get) match {
      case Some(worker) => worker
      case None => {
        val worker = allocatedWorker(
          (currentWorkerId.incrementAndGet() % allocatedWorker.length).abs)
        workerLocal.set(worker)
        worker
      }
    }
  }

  @`inline`
  def getServer(host: String): InetSocketAddress = {
    ucxServers.computeIfAbsent(host, _ => {
      logInfo(s"connecting $host with controller port")
      new InetSocketAddress(host, ucxServerPort)
    })
  }

  def connect(host: String, ports: Seq[Int]): Unit = {
    val server = ucxServers.computeIfAbsent(host, _ => {
      val id = executorId.toInt.abs % ports.length
      new InetSocketAddress(host, ports(id))
    })
    allocatedWorker.foreach(_.connect(server))
    logDebug(s"connect $host $server")
  }

  def connectAll(shuffleServerMap: Map[String, Seq[Int]]): Unit = {
    val addressSet = shuffleServerMap.map(hostPorts => {
      ucxServers.computeIfAbsent(hostPorts._1, _ => {
        val id = executorId.toInt.abs % hostPorts._2.length
        new InetSocketAddress(hostPorts._1, hostPorts._2(id))
      })
    }).toSeq
    allocatedWorker.foreach(_.connectAll(addressSet))
    logDebug(s"connectAll $addressSet")
  }

  def handleReplyAddress(msg: ByteBuffer): Unit = {
    val num = msg.remaining() / UnsafeUtils.INT_SIZE
    localServerPorts = (0 until num).map(_ => msg.getInt())
    localServerPortsBuffer = msg

    localServerPortsDone.countDown()

    connect(blockManagerId.host, localServerPorts)
  }

  /**
   * Batch version of [[ fetchBlocksByBlockIds ]].
   */
  def fetchBlocksByBlockIds(host: String, exeId: Int,
                            blockIds: Seq[BlockId],
                            callbacks: Seq[OperationCallback]): Unit = {
    selectWorker.fetchBlocksByBlockIds(host, exeId, blockIds, callbacks)
  }

  def fetchBlockByStream(host: String, exeId: Int, blockId: BlockId,
                         callback: OperationCallback): Unit = {
    selectWorker.fetchBlockByStream(host, exeId, blockId, callback)
  }

  def progressTimeOut(): Unit = {
    while (!scheduledLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      allocatedWorker.foreach(_.progressTimeOut)
    }
  }
}

private[shuffle] class UcxFetchCallBack(
  blockId: String, listener: BlockFetchingListener)
  extends OperationCallback {

  override def onComplete(result: OperationResult): Unit = {
    val memBlock = result.getData
    val buffer = UnsafeUtils.getByteBufferView(memBlock.address,
                                               memBlock.size.toInt)
    listener.onBlockFetchSuccess(blockId, new NioManagedBuffer(buffer) {
      override def release: ManagedBuffer = {
        memBlock.close()
        this
      }
    })
  }

  override def onError(result: OperationResult): Unit = {
    listener.onBlockFetchFailure(blockId, result.getError)
  }
}

private[shuffle] class UcxDownloadCallBack(
  blockId: String, listener: BlockFetchingListener,
  downloadFileManager: DownloadFileManager,
  transportConf: TransportConf)
  extends OperationCallback {

  private[this] val targetFile = downloadFileManager.createTempFile(
    transportConf)
  private[this] val channel = targetFile.openForWriting();

  override def onData(buffer: ByteBuffer): Unit = {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
  }

  override def onComplete(result: OperationResult): Unit = {
    listener.onBlockFetchSuccess(blockId, channel.closeAndRead());
    if (!downloadFileManager.registerTempFileToClean(targetFile)) {
      targetFile.delete();
    }
  }

  override def onError(result: OperationResult): Unit = {
    listener.onBlockFetchFailure(blockId, result.getError)
  }
}