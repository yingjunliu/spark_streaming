package org.apache.spark.monitor

import org.apache.spark.Logging
import org.apache.spark.monitor.JobMonitorMessages._
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import akka.actor._

import scala.collection.mutable.HashMap

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
      for (workerMonitor <- workerMonitors) {
        workerMonitor._2 ! StreamingBatchDuration(duration)
      }

  }

}
