/*
 * Copyright (C) 2022, NVIDIA CORPORATION & AFFILIATES. ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
package org.apache.spark.shuffle.ucx.perf

import java.io.{File, RandomAccessFile}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.cli.{GnuParser, HelpFormatter, Options}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.shuffle.ucx._
import org.apache.spark.shuffle.utils.UnsafeUtils
import org.apache.spark.util.{ShutdownHookManager, ThreadUtils}

import scala.collection.parallel.ForkJoinTaskSupport

object UcxPerfBenchmark extends App with Logging {

  case class PerfOptions(remoteAddress: InetSocketAddress, numBlocks: Int, blockSize: Long,
                         numIterations: Int, files: Array[File], numOutstanding: Int, randOrder: Boolean,
                         numThreads: Int)

  private val HELP_OPTION = "h"
  private val ADDRESS_OPTION = "a"
  private val FILE_OPTION = "f"
  private val NUM_BLOCKS_OPTION = "n"
  private val SIZE_OPTION = "s"
  private val ITER_OPTION = "i"
  private val OUTSTANDING_OPTION = "o"
  private val RANDREAD_OPTION = "r"
  private val THREAD_OPTION = "t"

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
    options.addOption(FILE_OPTION, "files", true, "Files to transfer")
    options.addOption(THREAD_OPTION, "thread", true, "Number of threads. Default: 1")
    options.addOption(RANDREAD_OPTION, "random", false, "Read blocks in random order")
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

    val files = if (cmd.hasOption(FILE_OPTION)) {
      cmd.getOptionValue(FILE_OPTION).split(",").map(f => new File(f))
    } else {
      Array.empty[File]
    }

    val randOrder = if (cmd.hasOption(RANDREAD_OPTION)) {
      true
    } else {
      false
    }

    PerfOptions(inetAddress,
      Integer.parseInt(cmd.getOptionValue(NUM_BLOCKS_OPTION, "1")),
      JavaUtils.byteStringAsBytes(cmd.getOptionValue(SIZE_OPTION, "1m")),
      Integer.parseInt(cmd.getOptionValue(ITER_OPTION, "1")),
      files,
      Integer.parseInt(cmd.getOptionValue(OUTSTANDING_OPTION, "1")),
      randOrder,
      Integer.parseInt(cmd.getOptionValue(THREAD_OPTION, "1")))
  }

  def startClient(options: PerfOptions): Unit = {
    if (options.numThreads > 1) {
      sparkConf.set("spark.executor.cores", options.numThreads.toString)
    }
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
    val rnd = new scala.util.Random
    val blocksPerFile = options.numBlocks / options.files.length

    val blockCollection =  if (options.numThreads > 1) {
      val parallelCollection = (0 until options.numBlocks by options.numOutstanding).par
      val threadPool = ThreadUtils.newForkJoinPool("Benchmark threads", options.numThreads)
      parallelCollection.tasksupport = new ForkJoinTaskSupport(threadPool)
      parallelCollection
    } else {
      0 until options.numBlocks by options.numOutstanding
    }

    for (_ <- 0 until options.numIterations) {
      for  (b <- blockCollection) {
        requestInFlight.set(options.numOutstanding)
        for (o <- 0 until options.numOutstanding) {
          val fileIdx = if (options.randOrder) rnd.nextInt(options.files.length) else (b+o) % options.files.length
          val blockIdx = if (options.randOrder) rnd.nextInt(blocksPerFile) else (b+o) % blocksPerFile
          blocks(o) = UcxShuffleBockId(0, fileIdx, blockIdx)
          callbacks(o) = (result: OperationResult) => {
            result.getData.close()
            val stats = result.getStats.get
            if (requestInFlight.decrementAndGet() == 0) {
              printf(s"Received ${options.numOutstanding} block of size: ${stats.recvSize}  " +
                s"in ${stats.getElapsedTimeNs / 1000} usec. Bandwidth: %.2f Mb/s \n",
                (options.blockSize * options.numOutstanding * options.numThreads) /
                  (1024.0 * 1024.0 * (stats.getElapsedTimeNs / 1e9)))
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

    if (options.files.isEmpty) {
      System.err.println(s"No file.")
      System.exit(-1)
    }
    options.files.foreach(f => if (!f.exists()) {
      System.err.println(s"File ${f.getPath} does not exist.")
      System.exit(-1)
    })

    val ucxTransport = new UcxShuffleTransport(new UcxShuffleConf(sparkConf), 0)
    ucxTransport.init()
    val currentThread = Thread.currentThread()

    var channels = Array[FileChannel]()
    options.files.foreach(channels +:= new RandomAccessFile(_, "r").getChannel)

    ShutdownHookManager.addShutdownHook(()=>{
      currentThread.interrupt()
      ucxTransport.close()
    })

    for (fileIdx <- options.files.indices) {
      for (blockIdx <- 0 until (options.numBlocks /  options.files.length)) {

        val blockId = UcxShuffleBockId(0, fileIdx, blockIdx)
        val block = new Block {
          private val channel = channels(fileIdx)
          private val fileOffset = blockIdx * options.blockSize

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
