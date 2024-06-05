package org.apache.spark.network.shuffle

import java.io.File

import org.apache.spark.network.server.OneForOneStreamManager
import org.apache.spark.network.util.TransportConf

import org.apache.spark.shuffle.utils.UcxLogging
import org.apache.spark.shuffle.ucx.ExternalUcxServerTransport

class ExternalUcxShuffleBlockHandler(conf: TransportConf, registeredExecutorFile: File)
  extends ExternalShuffleBlockHandler(new OneForOneStreamManager(),
  new ExternalUcxShuffleBlockResolver(conf, registeredExecutorFile)) with UcxLogging {
    def ucxBlockManager(): ExternalUcxShuffleBlockResolver = {
      blockManager.asInstanceOf[ExternalUcxShuffleBlockResolver]
    }
    def setTransport(transport: ExternalUcxServerTransport): Unit = {
      ucxBlockManager.setTransport(transport)
    }
}