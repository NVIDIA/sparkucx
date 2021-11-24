/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.perf

import java.io.{File, RandomAccessFile}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.shuffle.ucx._
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.ShutdownHookManager

object UcxPerfBenchmark extends App with Logging {

  case class PerfOptions(remoteAddress: InetSocketAddress, numBlocks: Int, blockSize: Long,
                         numIterations: Int, file: File, numOutstanding: Int)

  private val HELP_OPTION = "h"
  private val ADDRESS_OPTION = "a"
  private val FILE_OPTION = "f"
  private val NUM_BLOCKS_OPTION = "n"
  private val SIZE_OPTION = "s"
  private val ITER_OPTION = "i"
  private val OUTSTANDING_OPTION = "o"

  private val sparkConf = new SparkConf()

  private def initOptions(): Options = {
    val options = new Options()
    options.addOption(HELP_OPTION, "help", false,
      "display help message")
    options.addOption(ADDRESS_OPTION, "address", true,
      "address of the listener on the remote host")
    options.addOption(NUM_BLOCKS_OPTION, "num-blocks", true,
      "number of blocks to transfer. Default: 1")
    options.addOption(SIZE_OPTION, "block-size", true,
      "size of block to transfer. Default: 1m")
    options.addOption(ITER_OPTION, "num-iterations", true,
      "number of iterations. Default: 1")
    options.addOption(OUTSTANDING_OPTION, "num-outstanding", true,
      "number of outstanding requests. Default: 1")
    options.addOption(FILE_OPTION, "file", true, "File to transfer")
    options
  }

  private def parseOptions(args: Array[String]): PerfOptions = {
    val parser = new GnuParser()
    val options = initOptions()
    val cmd = parser.parse(options, args)

    if (cmd.hasOption(HELP_OPTION)) {
      new HelpFormatter().printHelp("UcxShufflePerfTool", options)
      System.exit(0)
    }

    val inetAddress = if (cmd.hasOption(ADDRESS_OPTION)) {
      val Array(host, port) = cmd.getOptionValue(ADDRESS_OPTION).split(":")
      new InetSocketAddress(host, Integer.parseInt(port))
    } else {
      null
    }

    val file = if (cmd.hasOption(FILE_OPTION)) {
      new File(cmd.getOptionValue(FILE_OPTION))
    } else {
      null
    }

    PerfOptions(inetAddress,
      Integer.parseInt(cmd.getOptionValue(NUM_BLOCKS_OPTION, "1")),
      JavaUtils.byteStringAsBytes(cmd.getOptionValue(SIZE_OPTION, "1m")),
      Integer.parseInt(cmd.getOptionValue(ITER_OPTION, "1")),
      file,
      Integer.parseInt(cmd.getOptionValue(OUTSTANDING_OPTION, "1")))
  }

  def startClient(options: PerfOptions): Unit = {
    val ucxTransport = new UcxShuffleTransport(new UcxShuffleConf(sparkConf), 0)
    ucxTransport.init()

    val hostString = options.remoteAddress.getHostString.getBytes(StandardCharsets.UTF_8)
    val address = ByteBuffer.allocateDirect(hostString.length + 4)
    address.putInt(options.remoteAddress.getPort)
    address.put(hostString)
    ucxTransport.addExecutor(1, address)

    val resultBufferAllocator = (size: Long) => ucxTransport.hostBounceBufferMemoryPool.get(size)
    val blocks = Array.ofDim[BlockId](options.numOutstanding)
    val callbacks = Array.ofDim[OperationCallback](options.numOutstanding)
    val requestInFlight = new AtomicInteger(0)

    for (_ <- 0 until options.numIterations) {
      for (b <- 0 until options.numBlocks by options.numOutstanding) {
        requestInFlight.set(options.numOutstanding)
        for (o <- 0 until options.numOutstanding) {
          blocks(o) = UcxShuffleBockId(0, 0, (b+o) % options.numBlocks)
          callbacks(o) = (result: OperationResult) => {
            result.getData.close()
            val stats = result.getStats.get
            if (requestInFlight.decrementAndGet() == 0) {
              printf(s"Received ${options.numOutstanding} block of size: ${stats.recvSize}  " +
                s"in ${stats.getElapsedTimeNs / 1000} usec. Bandwidth: %.2f Mb/s \n",
                (options.blockSize * options.numOutstanding) / (1024.0 * 1024.0 * (stats.getElapsedTimeNs / 1e9)))
            }
          }
        }
        val requests = ucxTransport.fetchBlocksByBlockIds(1, blocks, resultBufferAllocator, callbacks)
        while (!requests.forall(_.isCompleted)) {
          ucxTransport.progress()
        }
      }
    }
    ucxTransport.close()
  }

  def startServer(options: PerfOptions): Unit = {
    val ucxTransport = new UcxShuffleTransport(new UcxShuffleConf(sparkConf), 0)
    ucxTransport.init()
    val currentThread = Thread.currentThread()

    val channel = new RandomAccessFile(options.file, "rw").getChannel

    ShutdownHookManager.addShutdownHook(()=>{
      currentThread.interrupt()
      ucxTransport.close()
    })

    for (b <- 0 until options.numBlocks) {
      val blockId = UcxShuffleBockId(0, 0, b)
      val block = new Block {
        private val fileOffset = b * options.blockSize

        override def getMemoryBlock: MemoryBlock = {
          val startTime = System.nanoTime()
          val memBlock = ucxTransport.hostBounceBufferMemoryPool.get(options.blockSize)
          val dstBuffer = UnsafeUtils.getByteBufferView(memBlock.address, options.blockSize.toInt)
          channel.read(dstBuffer, fileOffset)
          logTrace(s"Read $blockId block of size: ${options.blockSize} in ${System.nanoTime() - startTime} ns")
          memBlock
        }

        override def getSize: Long = options.blockSize

        override def getBlock(byteBuffer: ByteBuffer): Unit = {
          channel.read(byteBuffer, fileOffset)
        }
      }
      ucxTransport.register(blockId, block)
    }
    while (!Thread.currentThread().isInterrupted) {
      Thread.sleep(10000)
    }
  }

  def start(): Unit = {
    val perfOptions = parseOptions(args)

    if (perfOptions.remoteAddress != null) {
      startClient(perfOptions)
    } else {
      startServer(perfOptions)
    }
  }

  start()
}
