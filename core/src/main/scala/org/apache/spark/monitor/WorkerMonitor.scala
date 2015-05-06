package org.apache.spark.monitor

import akka.actor.{ActorRef, Actor, Address, AddressFromURIString}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.Logging
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.util.ActorLogReceive

import scala.collection.mutable._

/**
 * Created by junjun on 2015/5/5.
 */
private[Spark] class WorkerMonitor
  extends Actor with ActorLogReceive with Logging {

  // The speed is Byte/ms
  private val totalHandleSpeed = new HashMap[String, Double]
  private val executors = new HashMap[String, ActorRef]

  override def preStart() = {
    logInfo("Start worker monitor")
  }

  override def receiveWithLogging = {
    case ExecutorHandledDataSpeed(size, executorId) =>
      totalHandleSpeed(executorId) += size

    case RegisterExecutor(executorId) =>
      executors(executorId) = sender
      logInfo(s"Registor executor ${executorId}")
      sender ! RegisteredExecutorInWorkerMonitor

    case StoppedExecutor(executorId) =>
      executors.remove(executorId)
      logInfo(s"Remove executor ${executorId}")
  }

}
