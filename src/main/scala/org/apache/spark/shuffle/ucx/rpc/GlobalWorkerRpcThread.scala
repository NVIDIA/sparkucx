/*
* Copyright (C) Mellanox Technologies Ltd. 2019. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.ucx.rpc

import org.openucx.jucx.ucp.{UcpAmData, UcpConstants, UcpEndpoint, UcpWorker}
import org.openucx.jucx.ucs.UcsConstants
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ucx.UcxShuffleTransport
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.ThreadUtils

class GlobalWorkerRpcThread(globalWorker: UcpWorker, transport: UcxShuffleTransport)
  extends Thread with Logging {
  setDaemon(true)
  setName("Global worker progress thread")

  private val threadPool = ThreadUtils.newDaemonFixedThreadPool(transport.ucxShuffleConf.numProgressThreads,
    "Progress threads")

  globalWorker.setAmRecvHandler(0, (headerAddress: Long, headerSize: Long, amData: UcpAmData,
                                     replyEp: UcpEndpoint) => {
    threadPool.submit(new Runnable() {
      private val startTag = UnsafeUtils.getByteBufferView(headerAddress, headerSize.toInt).getInt
      private val data = UnsafeUtils.getByteBufferView(amData.getDataAddress, amData.getLength.toInt)
      private val ep = replyEp

      override def run(): Unit = {
       transport.handleFetchBlockRequest(startTag, data, ep)
       amData.close()
      }
    })
    UcsConstants.STATUS.UCS_INPROGRESS
  }, UcpConstants.UCP_AM_FLAG_PERSISTENT_DATA | UcpConstants.UCP_AM_FLAG_WHOLE_MSG)

  override def run(): Unit = {
    if (transport.ucxShuffleConf.useWakeup) {
      while (!isInterrupted) {
        if (globalWorker.progress() == 0) {
          globalWorker.waitForEvents()
        }
      }
    } else {
      while (!isInterrupted) {
        globalWorker.progress()
      }
    }
  }
}
