package org.apache.spark.monitor

import scala.collection.mutable.HashMap

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] sealed trait WorkerMonitorMessage extends Serializable

private[spark] object WorkerMonitorMessages {

  // WorkerMonitor to Executor
  // Added by Liuzhiyi
  case object HandledDataSpeed

  case object RegisteredExecutorInWorkerMonitor

  // Executor to WorkerMonitor
  // Added by Liuzhiyi
  case class ExecutorHandledDataSpeed(size: Double, executorId: String) extends WorkerMonitorMessage

  case class RegisterExecutorInWorkerMonitor(executorId: String) extends WorkerMonitorMessage

  case class StoppedExecutor(executorId: String) extends WorkerMonitorMessage

  // Worker to WorkerMonitor
  //Added by Liuzhiyi
  case class RegisteredWorkerMonitor(workerId: String) extends WorkerMonitorMessage

  case class JobMonitorUrlForWorkerMonitor(url: String) extends WorkerMonitorMessage

  // WorkerMonitor to Worker
  // Added by Liuzhiyi
  case class RegisterWorkerMonitor(monitorAkkaUrls: String) extends WorkerMonitorMessage

  case object RequestJobMonitorUrlForWorkerMonitor

  // CoarseGrainedSchedulerBackend to WorkerMonitor
  // Added by Liuzhiyi
  case object RequestConnectionToWorkerMonitor

  case class PendingTaskAmount(amount: Int) extends WorkerMonitorMessage

  // WorkerMonitor to CoarseGrainedSchedulerBackend
  // Added by Liuzhiyi
  case class ConnectedWithWorkerMonitor(host: String) extends WorkerMonitorMessage
}

private[spark] sealed trait JobMonitorMessage extends Serializable

private[spark] object JobMonitorMessages {

  // JobMonitor to master
  case class RegisterJobMonitor(monitorAkkaUrl: String) extends JobMonitorMessage

  // master to JobMonitor
  case object RegisteredJobMonitor

  // Receiver to JobMonitor
  case class receivedDataSize(receiverId: Int, dataSize: Long) extends JobMonitorMessage

  // JobMonitor to BlockGenerator in spark streaming
  case class updateFunction(needSplit: Boolean, workerDataRatio: HashMap[String, Double]) extends JobMonitorMessage
}

private[spark] sealed trait MonitorMessage extends Serializable

private[spark] object MonitorMessages {

  // WorkerMonitor to JobMonitor
  // Added by Liuzhiyi
  case class RegisterWorkerMonitorInJobMonitor(workerId: String) extends MonitorMessage

  case class WorkerEstimateDataSize(dataSize: Long, workerId: String) extends MonitorMessage

  // JobMonitor to WorkerMonitor
  // Added by Liuzhiyi
  case object QueryEstimateDataSize

  case object RegisteredWorkerMonitorInJobMonitor
}
