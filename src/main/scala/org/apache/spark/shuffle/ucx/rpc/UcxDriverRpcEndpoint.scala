/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

class UcxDriverRpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints: mutable.Set[RpcEndpointRef] = mutable.HashSet.empty
  private var executorToWorkerAddress = HashMap.empty[Long, SerializableDirectBuffer]


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@ExecutorAdded(executorId: Long, endpoint: RpcEndpointRef,
    ucxWorkerAddress: SerializableDirectBuffer) => {
      // Driver receives a message from executor with it's workerAddress
      // 1. Introduce existing members of a cluster
      logDebug(s"Received $message")
      if (executorToWorkerAddress.nonEmpty) {
        val msg = IntroduceAllExecutors(executorToWorkerAddress)
        logDebug(s"replying $msg to $executorId")
        context.reply(msg)
      }
      executorToWorkerAddress += executorId -> ucxWorkerAddress
      // 2. For each existing member introduce newly joined executor.
      endpoints.foreach(ep => {
        logDebug(s"Sending $message to $ep")
        ep.send(message)
      })
      logDebug(s"Connecting back to address: ${context.senderAddress}")
      endpoints.add(endpoint)
    }
  }
}
