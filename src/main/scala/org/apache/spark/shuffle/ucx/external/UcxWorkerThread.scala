package org.apache.spark.shuffle.ucx

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentLinkedQueue
import org.openucx.jucx.ucp.UcpWorker

class UcxWorkerThread(worker: UcpWorker, useWakeup: Boolean) extends Thread {
  private val taskQueue = new ConcurrentLinkedQueue[Runnable]
  private val running = new AtomicBoolean(true)

  setDaemon(true)
  setName(s"UCX-worker-${super.getName}")

  @`inline`
  def post(task: Runnable): Unit = {
    taskQueue.add(task)
    worker.signal()
  }

  @`inline`
  def await() = {
    if (taskQueue.isEmpty) {
      worker.waitForEvents()
    }
  }

  @`inline`
  def close(cleanTask: Runnable): Unit = {
    if (running.compareAndSet(true, false)) {
      worker.signal()
    }
    cleanTask.run()
    worker.close()
  }

  override def run(): Unit = {
    val doAwait = if (useWakeup) await _ else () => {}
    while (running.get()) {
      val task = taskQueue.poll()
      if (task != null) {
        task.run()
      }
      while (worker.progress != 0) {}
      doAwait()
    }
  }
}