package org.apache.spark.shuffle.ucx

import org.openucx.jucx.ucp.UcpWorker

class UcxProgressThread(worker: UcpWorker, useWakeup: Boolean) extends Thread {
  setDaemon(true)
  setName(s"UCX-progress-${super.getName}")

  override def run(): Unit = {
    if (useWakeup) {
      while (!isInterrupted) {
        worker.synchronized {
          while (worker.progress != 0) {}
        }
        worker.waitForEvents()
      }
    } else {
      while (!isInterrupted) {
        worker.synchronized {
          while (worker.progress != 0) {}
        }
      }
    }
  }
}