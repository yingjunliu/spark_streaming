package org.apache.spark.monitor

import java.util.{Timer, TimerTask}

import akka.actor._

import org.apache.spark.Logging
import org.apache.spark.monitor.MonitorMessages._
import org.apache.spark.monitor.JobMonitorMessages._
import org.apache.spark.util.ActorLogReceive
import org.apache.spark.util.{AkkaUtils, ActorLogReceive}

import scala.collection.mutable.{HashMap, HashSet}

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
  val receivers = new HashMap[Int, ActorRef]
  val streamIdToSpeed = new HashMap[Int, Double]
  val hostToStreamId = new HashMap[String, HashSet[Int]]
  val workerToSpeed = new HashMap[String, Double]
  val DAGSchedulers = new HashMap[String, ActorRef]
  val jobTimes = new HashMap[Int, Double]
  val jobFinishReportTime = new HashMap[Long, Int]

  var hasSplit = false
  var workerMonitorReturnCount: Int = 0

  override def preStart() = {
    logInfo("Start job monitor")
    logInfo(s"Try to register job monitor to master ${master}")
    master ! RequestRegisterJobMonitor(monitorAkkaUrls)

//    val timer = new Timer()
//    val timerDelay = 2000
//    val timerPeriod = 1000
//    timer.schedule(new querySpeedTimerTask(workerMonitors.values.toArray), timerDelay, timerPeriod)
  }

  def splitOrNot(): Boolean = {
    false
  }

  override def receiveWithLogging = {
    // With workerMonitor
    case RequestRegisterWorkerMonitor(workerMonitorUrl, host) =>
      val workerMonitorActor = context.actorSelection(workerMonitorUrl)
      logInfo(s"Connection to worker monitor ${workerMonitorActor}")
      workerMonitorActor ! RegisterdWorkerMonitorInJobMonitor
      workerMonitors(host) = workerMonitorActor

    // With receiver
    case RequestRegisterReceiver(streamId) =>
      receivers(streamId) = sender
      logInfo(s"Registered receiver ${sender}")
      sender ! RegisteredReceiver

    case RequestRegisterDAGScheduler(appId) =>
      DAGSchedulers(appId) = sender
      logInfo(s"Registered DAGScheduler ${sender}")

    case StreamingReceiverSpeedToMonitor(streamId, speed, host) =>
      streamIdToSpeed(streamId) = speed
      hostToStreamId.getOrElseUpdate(host, new HashSet[Int]()) += streamId
      logInfo(s"streamId ${streamId}, speed ${speed}, host${host}")

    case WorkerHandledSpeed(host, speed) =>
//      workerToSpeed(host) = speed

    case JobFinished(jobId, runTime) =>
      jobTimes(jobId) = runTime
      logInfo(s"job ${jobId} runTime is ${runTime}")
      jobFinishReportTime(System.currentTimeMillis) = jobId

      for (workerMonitor <- workerMonitors.values) {
        workerMonitor ! QuaryWorkerHandledDataSize(jobId)
      }

    case WorkerHandledDataSize(host, size, jobId) =>
      workerToSpeed(host) = size / jobTimes(jobId)
      logInfo(s"worker ${host}, jobId is ${jobId}, size is ${size}, speed is ${workerToSpeed(host)}")
      workerMonitorReturnCount += 1
      if (hostToStreamId.contains(host)) {
        var totalSpeed: Double = 0.0
        for (streamId <- hostToStreamId(host)) {
          totalSpeed += streamIdToSpeed(streamId)
        }

//        logInfo(s"jobId ${jobId}, workerToSpeed ${workerToSpeed}, host ${host} totalSpeed ${totalSpeed}")

        if (totalSpeed > workerToSpeed(host) && (!hasSplit)) {
          for (streamId <- hostToStreamId(host)) {
            logInfo(s"split stream ${streamId}")
            receivers(streamId) ! SplitRecieverDataOrNot(streamId, true)
            hasSplit = true
          }
        }
      }
  }
}

//private[monitor] class querySpeedTimerTask(workerMonitorActors: Array[ActorSelection])
//                        extends TimerTask {
//  override def run(): Unit = {
//      for (workerMonitorActor <- workerMonitorActors) {
//        workerMonitorActor ! QuaryWorkerHandledSpeed
//    }
//  }
//}
