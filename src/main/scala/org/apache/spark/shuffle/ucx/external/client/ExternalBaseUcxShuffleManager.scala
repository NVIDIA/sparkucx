/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

import org.apache.spark.SparkException
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.ucx.rpc.{ExternalUcxExecutorRpcEndpoint, ExternalUcxDriverRpcEndpoint}
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{PushServiceAddress, PushAllServiceAddress}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer
import org.apache.spark.util.{RpcUtils, ThreadUtils}
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}
import org.openucx.jucx.{NativeLibs, UcxException}

/**
 * Common part for all spark versions for UcxShuffleManager logic
 */
abstract class ExternalBaseUcxShuffleManager(val conf: SparkConf, isDriver: Boolean) extends SortShuffleManager(conf) {
  type ShuffleId = Int
  type MapId = Int
  type ReduceId = Long
  
  /* Load UCX/JUCX libraries as soon as possible to avoid collision with JVM when register malloc/mmap hook. */
  if (!isDriver) {
    NativeLibs.load();
  }

  val ucxShuffleConf = new ExternalUcxClientConf(conf)

  @volatile var ucxTransport: ExternalUcxClientTransport = _

  private var executorEndpoint: ExternalUcxExecutorRpcEndpoint = _
  private var driverEndpoint: ExternalUcxDriverRpcEndpoint = _

  protected val driverRpcName = "SparkUCX_driver"

  private val setupThread = ThreadUtils.newDaemonSingleThreadExecutor("UcxTransportSetupThread")

  private[this] val latch = setupThread.submit(new Runnable {
    override def run(): Unit = {
      while (SparkEnv.get == null) {
        Thread.sleep(10)
      }
      if (isDriver) {
        val rpcEnv = SparkEnv.get.rpcEnv
        logInfo(s"Setting up driver RPC")
        driverEndpoint = new ExternalUcxDriverRpcEndpoint(rpcEnv)
        rpcEnv.setupEndpoint(driverRpcName, driverEndpoint)
      } else {
        while (SparkEnv.get.blockManager.shuffleServerId == null) {
          Thread.sleep(5)
        }
        startUcxTransport()
      }
    }
  })

  def awaitUcxTransport(): ExternalUcxClientTransport = {
    if (ucxTransport == null) {
      latch.get(10, TimeUnit.SECONDS)
      if (ucxTransport == null) {
        throw new UcxException("ExternalUcxClientTransport init timeout")
      }
    }
    ucxTransport
  }

  /**
   * Atomically starts UcxNode singleton - one for all shuffle threads.
   */
  def startUcxTransport(): Unit = if (ucxTransport == null) {
    val blockManager = SparkEnv.get.blockManager.shuffleServerId
    val transport = new ExternalUcxClientTransport(ucxShuffleConf, blockManager)
    transport.init()
    ucxTransport = transport
    val rpcEnv = SparkEnv.get.rpcEnv
    executorEndpoint = new ExternalUcxExecutorRpcEndpoint(rpcEnv, ucxTransport, setupThread)
    val endpoint = rpcEnv.setupEndpoint(
      s"ucx-shuffle-executor-${blockManager.executorId}",
      executorEndpoint)
    var driverCost = 0
    var driverEndpoint = RpcUtils.makeDriverRef(driverRpcName, conf, rpcEnv)
    while (driverEndpoint == null) {
      Thread.sleep(10)
      driverCost += 10
      driverEndpoint = RpcUtils.makeDriverRef(driverRpcName, conf, rpcEnv)
    }
    driverEndpoint.ask[PushAllServiceAddress](
      PushServiceAddress(blockManager.host, transport.localServerPorts, endpoint))
      .andThen {
        case Success(msg) =>
          logInfo(s"Driver take $driverCost ms.")
          executorEndpoint.receive(msg)
      }
  }


  override def unregisterShuffle(shuffleId: Int): Boolean = {
    // shuffleBlockResolver.asInstanceOf[CommonUcxShuffleBlockResolver].removeShuffle(shuffleId)
    super.unregisterShuffle(shuffleId)
  }

  /**
   * Called on both driver and executors to finally cleanup resources.
   */
  override def stop(): Unit = synchronized {
    super.stop()
    if (ucxTransport != null) {
      ucxTransport.close()
      ucxTransport = null
    }
    if (executorEndpoint != null) {
      executorEndpoint.stop()
    }
    if (driverEndpoint != null) {
      driverEndpoint.stop()
    }
    setupThread.shutdown()
  }

}