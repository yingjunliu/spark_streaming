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

  case class RegisterExecutor(executorId: String) extends MonitorMessage

  case class StoppedExecutor(executorId: String) extends MonitorMessage

}
