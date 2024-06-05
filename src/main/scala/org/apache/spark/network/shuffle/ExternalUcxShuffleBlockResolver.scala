package org.apache.spark.network.shuffle

import java.io.File
import java.nio.charset.StandardCharsets
import java.lang.reflect.{Method, Field}

import scala.collection.mutable

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo
import org.apache.spark.network.util.TransportConf
import org.apache.spark.network.shuffle.ExternalShuffleBlockResolver.AppExecId

import org.apache.spark.shuffle.utils.UcxLogging
import org.apache.spark.shuffle.ucx.ExternalUcxServerTransport

class ExternalUcxShuffleBlockResolver(conf: TransportConf, registeredExecutorFile: File)
  extends ExternalShuffleBlockResolver(conf, registeredExecutorFile) with UcxLogging {
  private[spark] final val APP_KEY_PREFIX = "AppExecShuffleInfo";
  private[spark] final val ucxMapper = new ObjectMapper
  private[spark] var dbAppExecKeyMethod: Method = _
  private[spark] val knownManagers = mutable.Set(
    "org.apache.spark.shuffle.sort.SortShuffleManager",
    "org.apache.spark.shuffle.unsafe.UnsafeShuffleManager",
    "org.apache.spark.shuffle.ExternalUcxShuffleManager")
  private[spark] var ucxTransport: ExternalUcxServerTransport = _

  // init()

  private[spark] def dbAppExecKey(appExecId: AppExecId): Array[Byte] = {
    // we stick a common prefix on all the keys so we can find them in the DB
    val appExecJson = ucxMapper.writeValueAsString(appExecId);
    val key = (APP_KEY_PREFIX + ";" + appExecJson);
    key.getBytes(StandardCharsets.UTF_8);
  }

  // def init(): Unit = {
  //   val clazz = Class.forName("org.apache.spark.network.shuffle.ExternalShuffleBlockResolver")
  //   try {
  //     dbAppExecKeyMethod = clazz.getDeclaredMethod("dbAppExecKey", classOf[AppExecId])
  //     dbAppExecKeyMethod.setAccessible(true)
  //   } catch {
  //     case e: Exception => {
  //       logError(s"Get dbAppExecKey from ExternalUcxShuffleBlockResolver failed: $e")
  //     }
  //   }
  // }

  // def dbAppExecKey(fullId: AppExecId): Array[Byte] = {
  //   dbAppExecKeyMethod.invoke(this, fullId).asInstanceOf[Array[Byte]]
  // }

  def setTransport(transport: ExternalUcxServerTransport): Unit = {
    ucxTransport = transport
  }
  /** Registers a new Executor with all the configuration we need to find its shuffle files. */
  override def registerExecutor(
      appId: String,
      execId: String,
      executorInfo: ExecutorShuffleInfo): Unit = {
    val fullId = new AppExecId(appId, execId)
    logInfo(s"Registered executor ${fullId} with ${executorInfo}")
    if (!knownManagers.contains(executorInfo.shuffleManager)) {
      throw new UnsupportedOperationException(
        "Unsupported shuffle manager of executor: " + executorInfo)
    }
    try {
      if (db != null) {
        val key = dbAppExecKey(fullId)
        val value = ucxMapper.writeValueAsString(executorInfo).getBytes(StandardCharsets.UTF_8)
        db.put(key, value)
      }
      executors.put(fullId, executorInfo)
    } catch {
      case e: Exception => logError("Error saving registered executors", e)
    }
  }

  override def applicationRemoved(appId: String, cleanupLocalDirs: Boolean): Unit = {
    super.applicationRemoved(appId, cleanupLocalDirs)
    ucxTransport.applicationRemoved(appId)
  }
  override def executorRemoved(executorId: String, appId: String): Unit = {
    super.executorRemoved(executorId, appId)
    ucxTransport.executorRemoved(executorId, appId)
  }
}