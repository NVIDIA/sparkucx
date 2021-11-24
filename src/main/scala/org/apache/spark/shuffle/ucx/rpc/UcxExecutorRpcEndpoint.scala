/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{ExecutorAdded, IntroduceAllExecutors}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

class UcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, transport: UcxShuffleTransport)
  extends RpcEndpoint  with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case ExecutorAdded(executorId: Long, _: RpcEndpointRef,
    ucxWorkerAddress: SerializableDirectBuffer) =>
      logDebug(s"Received ExecutorAdded($executorId)")
      transport.addExecutor(executorId, ucxWorkerAddress.value)
      transport.progressAll()
    case IntroduceAllExecutors(executorIdToWorkerAdresses: Map[Long, SerializableDirectBuffer]) =>
      logDebug(s"Received IntroduceAllExecutors(${executorIdToWorkerAdresses.keys.mkString(",")}")
      transport.addExecutors(executorIdToWorkerAdresses)
      transport.preConnect()
      transport.progressAll()
  }
}
