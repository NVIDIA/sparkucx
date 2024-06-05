/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.net.InetSocketAddress

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.shuffle.ucx.rpc.UcxRpcMessages.{PushServiceAddress, PushAllServiceAddress}
import org.apache.spark.shuffle.ucx.utils.{SerializableDirectBuffer, SerializationUtils}

class ExternalUcxDriverRpcEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {

  private val endpoints = mutable.HashSet.empty[RpcEndpointRef]
  private var shuffleServerMap = mutable.HashMap.empty[String, Seq[Int]]


  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case message@PushServiceAddress(host: String, ports: Seq[Int], endpoint: RpcEndpointRef) => {
      // Driver receives a message from executor with it's workerAddress
      logInfo(s"Received message $ports from ${context.senderAddress}")
      // 1. Introduce existing members of a cluster
      if (shuffleServerMap.nonEmpty) {
        val msg = PushAllServiceAddress(shuffleServerMap.toMap)
        logDebug(s"Replying $msg to $endpoint")
        context.reply(msg)
      }
      // 2. For each existing member introduce newly joined executor.
      if (!shuffleServerMap.contains(host)) {
        shuffleServerMap += host -> ports
        endpoints.foreach(ep => {
          logDebug(s"Sending message $ports to $ep")
          ep.send(message)
        })
      }
      // 3. Add ep to registered eps.
      endpoints.add(endpoint)
    }
  }
}