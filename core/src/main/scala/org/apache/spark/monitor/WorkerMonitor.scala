package org.apache.spark.monitor

import java.util.{Timer, TimerTask}

import akka.actor._

import org.apache.spark.Logging
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.monitor.WorkerMonitorMessages._
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
  private var schedulerBackend: ActorRef = null
  private var jobMonitor: ActorSelection = null

  override def preStart() = {
    logInfo("Start worker monitor")
    logInfo(s"Connection to the worker ${worker.path.address}")
    worker ! RegisterWorkerMonitor(actorAkkaUrls)

    /*
     * Set timer task to query executor's handle speed.
     * When monitor set, the timer task will delay {timerDelay} times
     * Evey {timerPeriod} time will query once.
     */
    val timer = new Timer()
    val timerDelay = 2000
    val timerPeriod = 1000
    timer.schedule(new WorkerQuerySpeedTimerTask(executors), timerDelay, timerPeriod)
  }

  override def receiveWithLogging = {
    case RegistedWorkerMonitor =>
      logInfo("Registed worker monitor")

    case SendJobMonitorUrl(url) =>
      jobMonitor = context.actorSelection(url)
      jobMonitor ! RequestRegisterWorkerMonitor(actorAkkaUrls, host)

    case ExecutorHandledDataSpeed(size, executorId) =>
      totalHandleSpeed(executorId) = size
      logInfo(s"The handle speed in executor ${executorId} is ${size}")

    case RegisterExecutorWithMonitor(executorId) =>
      executors(executorId) = sender
      logInfo(s"Registor executor ${executorId}")
      sender ! RegisteredExecutorInWorkerMonitor

    case StoppedExecutor(executorId) =>
      executors.remove(executorId)
      logInfo(s"Remove executor ${executorId}")

    case RegistedWorkerMonitorInSchedulerBackend =>
      schedulerBackend = sender
      logInfo(s"Registerd scheduler backend ${sender}")

    case QuaryWorkerHandledSpeed =>
      var totalSpeed: Double = 0.0
      for (speed <- totalHandleSpeed.valuesIterator) {
        totalSpeed += speed
      }
      sender ! WorkerHandledSpeed(host, totalSpeed)
  }

}

private[monitor] class WorkerQuerySpeedTimerTask(executors: HashMap[String, ActorRef]) extends TimerTask {
  override def run(): Unit = {
    for(executor <- executors.valuesIterator) {
      executor ! HandledDataSpeed
    }
  }
}
