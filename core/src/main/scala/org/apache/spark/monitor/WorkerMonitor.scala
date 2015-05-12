package org.apache.spark.monitor

import akka.actor.{ActorRef, Actor, Address, AddressFromURIString}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.Logging
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import scala.collection.mutable._

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
  private val totalHandleSpeed = new HashMap[String, Double]
  private val executors = new HashMap[String, ActorRef]
  private val actorAkkaUrls = AkkaUtils.address(
    AkkaUtils.protocol(context.system),
    actorSystemName,
    host,
    port,
    actorName)

  override def preStart() = {
    logInfo("Start worker monitor")
    logInfo("Connection to the worker ")
    worker ! RegisterWorkerMonitor(actorAkkaUrls)
  }

  override def receiveWithLogging = {
    case RegistedWorkerMonitor =>
      logInfo("Registed worker monitor")

    case ExecutorHandledDataSpeed(size, executorId) =>
      totalHandleSpeed(executorId) += size

    case RegisterExecutorWithMonitor(executorId) =>
      executors(executorId) = sender
      logInfo(s"Registor executor ${executorId}")
      sender ! RegisteredExecutorInWorkerMonitor

    case StoppedExecutor(executorId) =>
      executors.remove(executorId)
      logInfo(s"Remove executor ${executorId}")
  }

}
