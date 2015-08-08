package org.apache.spark.monitor

import akka.actor._
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.Logging
import org.apache.spark.monitor.WorkerMonitorMessages._
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import scala.collection.mutable.{HashMap, HashSet}

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] class WorkerMonitor(
       worker: ActorRef,
       actorSystemName: String,
       host: String,
       port: Int,
       actorName: String)
  extends Actor with ActorLogReceive with Logging {

  // The speed is Byte/ms
  private val executorHandleSpeed = new HashMap[String, Double]
  private val executors = new HashMap[String, ActorRef]
  private val actorAkkaUrls = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)
  private var workerId = ""
  private var jobMonitor: ActorSelection = null
  private val schedulerBackendToTasks = new HashMap[ActorRef, HashSet[Long]]
  private var totalPendingTask = 0

  override def preStart() = {
    logInfo("Start worker monitor")
    logInfo("Connection to the worker ")
    worker ! RegisterWorkerMonitor(actorAkkaUrls)
  }

  override def receiveWithLogging = {
    case RegisteredWorkerMonitor(registeredWorkerId) =>
      workerId = registeredWorkerId
      logInfo("Registered worker monitor")
      worker ! RequestJobMonitorUrlForWorkerMonitor

    case JobMonitorUrlForWorkerMonitor(url) =>
      jobMonitor = context.actorSelection(url)
      jobMonitor ! RegisterWorkerMonitorInJobMonitor(workerId)

    case RegisteredWorkerMonitorInJobMonitor =>
      logInfo(s"Registered in job monitor ${sender}")

    case ExecutorHandledDataSpeed(size, executorId) =>
      executorHandleSpeed(executorId) = size
      totalPendingTask -= 1

    case RegisterExecutorInWorkerMonitor(executorId) =>
      executors(executorId) = sender
      logInfo(s"Register executor ${executorId}")
      sender ! RegisteredExecutorInWorkerMonitor

    case StoppedExecutor(executorId) =>
      executors.remove(executorId)
      logInfo(s"Stopped executor ${executorId}")

    case RequestConnectionToWorkerMonitor =>
      schedulerBackendToTasks(sender) = new HashSet[Long]
      logInfo(s"connected to scheduler backend ${sender}")
      sender ! ConnectedWithWorkerMonitor(host)

    case PendingTaskAmount(amount) =>
      totalPendingTask += amount
  }

  private def forecaseDataSize(): Long = {
    0L
  }

}
