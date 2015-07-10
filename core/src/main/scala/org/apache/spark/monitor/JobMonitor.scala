package org.apache.spark.monitor

import java.util.{Timer, TimerTask}

import akka.actor._

import org.apache.spark.Logging
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.monitor.JobMonitorMessages._
import org.apache.spark.util.ActorLogReceive
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import scala.collection.mutable.HashMap

/**
 * Created by junjun on 2015/7/10.
 */
private[spark] class JobMonitor(
       master: ActorRef,
       actorSystemName: String,
       host: String,
       port: Int,
       actorName: String)
  extends Actor with ActorLogReceive with Logging {

  val workerMonitors = new HashMap[String, ActorSelection]
  val monitorAkkaUrls = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)
  val receivers = new HashMap[String, ActorRef]

  override def preStart() = {
    logInfo("Start job monitor")
    logInfo(s"Try to register job monitor to master ${master}")
    master ! RequestRegisterJobMonitor(monitorAkkaUrls)
  }

  override def receiveWithLogging = {
    // With workerMonitor
    case RequestRegisterWorkerMonitor(workerMonitorUrl, host) =>
      val workerMonitorActor = context.actorSelection(workerMonitorUrl)
      logInfo(s"Connection to worker monitor ${workerMonitorActor}")
      workerMonitorActor ! RegisterdWorkerMonitorInJobMonitor
      workerMonitors(host) = workerMonitorActor

    // With receiver
    case RequestRegisterReceiver(receiverId) =>
      receivers(receiverId) = sender
      logInfo(s"Registered receiver ${sender}")
      sender ! RegisteredReceiver
  }
}
