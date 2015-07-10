package org.apache.spark.monitor

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] sealed trait MonitorMessage extends Serializable

private [spark] object MonitorMessages {
  case class RequestRegisterWorkerMonitor(workerMonitorUrl: String, host: String) extends MonitorMessage

  case object RegisterdWorkerMonitorInJobMonitor extends MonitorMessage

  case object QuaryWorkerHandledSpeed extends MonitorMessage

  case class WorkerHandledSpeed(host: String, speed: Double) extends MonitorMessage

  case object QuaryWorkerHandledDataSize extends MonitorMessage

  case class WorkerHandledDataSize(host: String, size: Long) extends MonitorMessage
}

private[spark] sealed trait WorkerMonitorMessage extends Serializable

private[spark] object WorkerMonitorMessages {

  // WorkerMonitor to Executor
  // Added by Liuzhiyi
  case object HandledDataSpeed

  case object RegisteredExecutorInWorkerMonitor

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

  case class HandledSpeedInWorkerMonitor(host: String,
                                        handleSpeed: Double) extends WorkerMonitorMessage

  case class SendJobMonitorUrl(url: String) extends WorkerMonitorMessage
}

private[spark] sealed trait JobMonitorMessage extends Serializable

private[spark] object JobMonitorMessages {
  case class RequestRegisterJobMonitor(monitorAkkaUrls: String) extends JobMonitorMessage

  case class RequestRegisterReceiver(receiverId: String) extends JobMonitorMessage

  case object RegisteredReceiver extends JobMonitorMessage
}
