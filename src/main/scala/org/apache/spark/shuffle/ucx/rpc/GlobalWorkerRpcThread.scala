/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import java.nio.ByteBuffer
import org.openucx.jucx.ucp.{UcpAmData, UcpConstants, UcpEndpoint, UcpWorker}
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.{UcxShuffleTransport, UcxShuffleBockId}
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.ThreadUtils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, transport: UcxShuffleTransport)
  extends Thread with Logging {
  setDaemon(true)
  setName("Global worker progress thread")

  // Main RPC thread. Submit each RPC request to separate thread and send reply back from separate worker.
  globalWorker.setAmRecvHandler(0, (headerAddress: Long, headerSize: Long, amData: UcpAmData, _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val replyTag = header.getInt
    val replyExecutor = header.getLong
    val buffer = UnsafeUtils.getByteBufferView(amData.getDataAddress,
                                               amData.getLength.toInt)
    val blockNum = buffer.remaining() / UcxShuffleBockId.serializedSize
    val blockIds = (0 until blockNum).map(
      _ => UcxShuffleBockId.deserialize(buffer))
    transport.handleFetchBlockRequest(replyTag, blockIds, replyExecutor)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG )


  // AM to get worker address for client worker and connect server workers to it
  globalWorker.setAmRecvHandler(1, (headerAddress: Long, headerSize: Long, amData: UcpAmData,
                                    _: UcpEndpoint) => {
    val header = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt)
    val executorId = header.getLong
    val workerAddress = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
    transport.connectServerWorkers(executorId, workerAddress)
    UcsConstants.STATUS.UCS_OK
  }, UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  override def run(): Unit = {
    if (transport.ucxShuffleConf.useWakeup) {
      while (!isInterrupted) {
        while (globalWorker.progress != 0) {}
        globalWorker.waitForEvents()
      }
    } else {
      while (!isInterrupted) {
        while (globalWorker.progress != 0) {}
      }
    }
  }
}
