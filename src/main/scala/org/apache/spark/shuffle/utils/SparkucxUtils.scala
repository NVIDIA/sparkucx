/*
* Copyright (C) Mellanox Technologies Ltd. 2020. ALL RIGHTS RESERVED.
* See file LICENSE for terms.
*/
package org.apache.spark.shuffle.utils

import java.io.IOException
import java.math.{MathContext, RoundingMode}
import java.util.Locale
import java.util.concurrent.ThreadFactory
import scala.util.control.NonFatal

object SparkucxUtils extends UcxLogging {
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EB = 1L << 60
    val PB = 1L << 50
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    if (size >= BigInt(1L << 11) * EB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EB) {
          (BigDecimal(size) / EB, "EB")
        } else if (size >= 2 * PB) {
          (BigDecimal(size) / PB, "PB")
        } else if (size >= 2 * TB) {
          (BigDecimal(size) / TB, "TB")
        } else if (size >= 2 * GB) {
          (BigDecimal(size) / GB, "GB")
        } else if (size >= 2 * MB) {
          (BigDecimal(size) / MB, "MB")
        } else if (size >= 2 * KB) {
          (BigDecimal(size) / KB, "KB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }
}

class UcxThreadFactory extends ThreadFactory {
  private var daemon: Boolean = true
  private var prefix: String = "UCX"

  private class NamedThread(r: Runnable) extends Thread(r) {
    setDaemon(daemon)
    setName(s"${prefix}-${super.getName}")
  }

  def setDaemon(isDaemon: Boolean): this.type = {
    daemon = isDaemon
    this
  }

  def setPrefix(name: String): this.type = {
    prefix = name
    this
  }

  def newThread(r: Runnable): Thread = {
    new NamedThread(r)
  }
}