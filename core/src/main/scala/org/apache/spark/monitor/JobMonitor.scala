package org.apache.spark.monitor

import org.apache.spark.Logging
import org.apache.spark.monitor.JobMonitorMessages._
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import akka.actor._

import scala.collection.mutable.{ArrayBuffer, HashMap}

import java.util.{Timer, TimerTask}

/**
 * Created by handsome on 2015/8/4.
 */
private[spark] class JobMonitor(master: ActorRef,
                                actorSystemName: String,
                                host: String,
                                port: Int,
                                actorName: String)
  extends Actor with ActorLogReceive with Logging {

  val jobMonitorAkkaUrl = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)
  val workerMonitors = new HashMap[String, ActorRef]
  var batchDuration = 0L
  val pendingDataSizeForHost = new HashMap[String, Long]
  val workerEstimateDataSize = new HashMap[String, Long]
  val workerToHost = new HashMap[String, String]
  var receiverTracker: ActorRef = null
  var timer: Timer = null

  override def preStart() = {
    logInfo("Start job monitor")
    master ! RegisterJobMonitor(jobMonitorAkkaUrl)
  }

  override def receiveWithLogging = {
    // With master
    case RegisteredJobMonitor =>
      logInfo(s"Registed jobMonitor in master ${sender}")

    case RegisterWorkerMonitorInJobMonitor(workerId) =>
      workerMonitors(workerId) = sender
      sender ! RegisteredWorkerMonitorInJobMonitor

    case BatchDuration(duration) =>
      receiverTracker = sender
      logInfo(s"test - The batch duration is ${duration}")
      for (workerMonitor <- workerMonitors) {
        workerMonitor._2 ! StreamingBatchDuration(duration)
        batchDuration = duration
      }

    case ReceivedDataSize(host, size) =>
      if (pendingDataSizeForHost.contains(host)) {
        pendingDataSizeForHost(host) += size
      } else {
        pendingDataSizeForHost(host) = size
      }

    case JobFinished(time) =>
      if (pendingDataSizeForHost.size != 0) {
        for (workerMonitor <- workerMonitors) {
          workerMonitor._2 ! QueryEstimateDataSize
        }
        timer = new Timer()
        timer.schedule(new updateDataLocation(), batchDuration / 3, batchDuration * 2)
      }

    case WorkerEstimateDataSize(estimateDataSize, handledDataSize, workerId, host) =>
      if (!pendingDataSizeForHost.contains(host)) {
        pendingDataSizeForHost(host) = 0L
      }
      pendingDataSizeForHost(host) -= handledDataSize
      workerEstimateDataSize(workerId) = estimateDataSize
      workerToHost(workerId) = host
      logInfo(s"test - Pending data size for host ${pendingDataSizeForHost}")
  }

  def sendDataToCertainLocation(hostList: ArrayBuffer[(String, Long)]) = {
    val result = new HashMap[String, Double]
    if (hostList(2)._2 == 0) {
      result(hostList(0)._1) = 1.0 / 3
      result(hostList(1)._1) = 1.0 / 3
      result(hostList(2)._1) = 1.0 / 3
    } else if (hostList(0)._2 != 0) {
      val allSize = hostList(0)._2 + hostList(1)._2 + hostList(2)._2
      result(hostList(0)._1) = hostList(0)._2.toDouble / allSize
      result(hostList(1)._1) = hostList(1)._2.toDouble / allSize
      result(hostList(2)._1) = hostList(2)._2.toDouble / allSize
    } else if (hostList(1)._2 == 0) {
      result(hostList(0)._1) = 0.4
      result(hostList(1)._1) = 0.4
      result(hostList(2)._1) = 0.8
    } else {
      val allSize = hostList(1)._2 + hostList(2)._2
      result(hostList(0)._1) = 0.4
      result(hostList(1)._1) = (hostList(1)._2.toDouble / allSize) * 0.6
      result(hostList(2)._1) = (hostList(2)._2.toDouble / allSize) * 0.6
    }
    logInfo(s"test - data reallocate result ${result}")
    receiverTracker ! DataReallocateTable(result)
    timer.cancel()
  }

  private class updateDataLocation() extends TimerTask {
    override def run() = {
      val hostList = new ArrayBuffer[(String, Long)]
      val hostToEstimateDataSize = new HashMap[String, Long]
      for (worker <- workerToHost) {
        hostToEstimateDataSize(host) = hostToEstimateDataSize.getOrElseUpdate(worker._2, 0L) + workerEstimateDataSize(worker._1)
      }
      for (zeroHost <- hostToEstimateDataSize) {
        if (zeroHost._2 == 0L) {
          hostList.append(zeroHost)
          hostToEstimateDataSize.remove(zeroHost._1)
        }
      }
      val size = hostToEstimateDataSize.size
      for (i <- 0 until size) {
        var max:(String, Long) = ("", 0L)
        for (line <- hostToEstimateDataSize) {
          if (line._2 > max._2) {
            max = line
          }
        }
        hostList.append(max)
        hostToEstimateDataSize.remove(max._1)
      }

      sendDataToCertainLocation(hostList.take(3))

    }
  }

}
