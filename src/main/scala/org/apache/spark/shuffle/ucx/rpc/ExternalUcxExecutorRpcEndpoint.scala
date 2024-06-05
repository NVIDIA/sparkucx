/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.shuffle.ucx.ExternalUcxClientTransport
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{PushServiceAddress, PushAllServiceAddress}
import org.apache.spark.shuffle.ucx.utils.SerializableDirectBuffer

import java.util.concurrent.ExecutorService

class ExternalUcxExecutorRpcEndpoint(override val rpcEnv: RpcEnv, transport: ExternalUcxClientTransport,
                             executorService: ExecutorService)
  extends RpcEndpoint {

  override def receive: PartialFunction[Any, Unit] = {
    case PushServiceAddress(host: String, ports: Seq[Int], _: RpcEndpointRef) =>
      transport.connect(host, ports)
    case PushAllServiceAddress(shuffleServerMap: Map[String, Seq[Int]]) =>
      transport.connectAll(shuffleServerMap)
  }
}