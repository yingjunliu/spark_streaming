package org.apache.spark.monitor

import scala.collection.mutable._

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] sealed trait MonitorMessage extends Serializable

private [spark] object MonitorMessages {
  case class RequestRegisterWorkerMonitor(workerMonitorUrl: String, host: String) extends MonitorMessage

  case object RegisterdWorkerMonitorInJobMonitor

  case object QuaryWorkerHandledSpeed

  case class WorkerHandledSpeed(host: String, speed: Double) extends MonitorMessage

  case class QuaryWorkerHandledDataSize(jobId: Int) extends MonitorMessage

  case class WorkerHandledDataSize(host: String, size: Long, jobId: Int) extends MonitorMessage
}

private[spark] sealed trait WorkerMonitorMessage extends Serializable

private[spark] object WorkerMonitorMessages {

  // WorkerMonitor to Executor
  // Added by Liuzhiyi
  case object HandledDataSpeed

  case object RegisteredExecutorInWorkerMonitor

  case class QuaryHandledDataSize(stageIdToTaskIds: HashMap[Int, HashSet[Long]])
    extends WorkerMonitorMessage

  // Executor to WorkerMonitor
  // Added by Liuzhiyi
  case class ExecutorHandledDataSpeed(size: Double, executorId: String) extends WorkerMonitorMessage

  case class RegisterExecutorWithMonitor(executorId: String) extends WorkerMonitorMessage

  case class StoppedExecutor(executorId: String) extends WorkerMonitorMessage


  // Worker to WorkerMonitor
  //Added by Liuzhiyi
  case object RegistedWorkerMonitor

  //WorkerMonitor to Worker
  //Added by Liuzhiyi
  case class RegisterWorkerMonitor(MonitorAkkaUrls: String) extends WorkerMonitorMessage

  //CoarseGrainedSchedulerBackend to WorkerMonitor
  case object RegistedWorkerMonitorInSchedulerBackend

  case object QuaryHandledSpeed

  case class SendJobMonitorUrl(url: String) extends WorkerMonitorMessage

  case class HandledDataInExecutor(jobId: Int, dataSize: Long) extends WorkerMonitorMessage
}

private[spark] sealed trait JobMonitorMessage extends Serializable

private[spark] object JobMonitorMessages {
  case class RequestRegisterJobMonitor(monitorAkkaUrls: String) extends JobMonitorMessage

  case class RequestRegisterReceiver(streamId: Int) extends JobMonitorMessage

  case class RequestRegisterDAGScheduler(appId: String) extends JobMonitorMessage

  case object RegisteredReceiver

  case class StreamingReceiverSpeedToMonitor(streamId: Int, speed: Double, host: String) extends JobMonitorMessage

  case class JobFinished(jobId: Int,
                         startTime: Long,
                         endTime: Long)
    extends JobMonitorMessage
}
