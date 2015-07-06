package org.apache.spark.monitor

/**
 * Created by junjun on 2015/5/5.
 */
private[spark] sealed trait MonitorMessage extends Serializable

private[spark] object MonitorMessages {

  // WorkerMonitor to Executor
  // Added by Liuzhiyi
  case object HandledDataSpeed

  case object RegisteredExecutorInWorkerMonitor

  // Executor to WorkerMonitor
  // Added by Liuzhiyi
  case class ExecutorHandledDataSpeed(size: Double, executorId: String) extends MonitorMessage

  case class RegisterExecutorWithMonitor(executorId: String) extends MonitorMessage

  case class StoppedExecutor(executorId: String) extends MonitorMessage


  // Worker to WorkerMonitor
  //Added by Liuzhiyi
  case object RegistedWorkerMonitor

  //WorkerMonitor to Worker
  //Added by Liuzhiyi
  case class RegisterWorkerMonitor(MonitorAkkaUrls: String) extends MonitorMessage

  //CoarseGrainedSchedulerBackend to WorkerMonitor
  case object RegistedWorkerMonitorInSchedulerBackend

  case object QuaryHandledSpeed

  case class HandledSpeedInWorkerMonitor(host: String,
                                        handleSpeed: Double) extends MonitorMessage
}
