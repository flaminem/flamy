/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.flaminem.flamy.exec.utils

import com.flaminem.flamy.conf.{FlamyContext, FlamyGlobalContext}
import com.flaminem.flamy.exec.utils.Workflow.Status
import com.flaminem.flamy.utils.logging.Logging

/**
  * A ParallelActionRunner offers the capability to execute multiple actions in parallel.
  * The configuration parameter flamy.exec.parallelism allows the user to specify a maximal number of concurrent actions.
  * When the number of actions to runner is greater than the level of parallelism, priority is determined by an implicit Ordering over the actions.
  *
  * The user can override the method prepareNextActions to add new actions to the current workflow after each action completion.
  * The user can also override the method delayAction to add a lock system preventing some type of actions to run concurrently.
  */
class ParallelActionRunner[A <: Action](val context: FlamyContext)(implicit val actionOrdering: Ordering[A]) extends AutoCloseable with Logging {
  protected val workflow = new Workflow[A]

  private var interrupted = false

  val feedback: ActionFeedback = {
    if(FlamyGlobalContext.DYNAMIC_OUTPUT.getProperty){
      new MultilineActionFeedback
    }
    else {
      new SimpleActionFeedback
    }
  }

  private def startTodoJobs() {
    for {
      action <- workflow.getTodoJobs.sorted(actionOrdering)
      if workflow.getRunningJobs.size < context.PARALLELISM.getProperty
      if !delayAction(action)
    } {
      if(action.isInstanceOf[SkipAction]){
        workflow.skipped(action)
        feedback.skipped(action)
        todoNextActions(action)
      }
      else {
        workflow.running(action)
        feedback.started(action)
        ActionRunner.runInParallel(action)
      }
    }
  }

  private def todoNextActions(action: A) = {
    for {
      action <- getNextActions(action)
    } {
      workflow.todo(action)
    }
  }

  private def cleanFinishedJobs(): ReturnStatus = {
    val results: Seq[ReturnStatus] =
      for {
        action <- workflow.getRunningJobs
        if action.isFinished
      } yield {
        action.result.get match {
          case ActionResult(ReturnSuccess, _, _) =>
            workflow.successful(action)
            feedback.successful(action)
            todoNextActions(action)
            ReturnSuccess
          case res @ ActionResult(ReturnFailure, logPath, Some(e: InterruptedException)) =>
            workflow.failed(action)
            ReturnFailure
          case res @ ActionResult(ReturnFailure, logPath, _) =>
            workflow.failed(action)
            feedback.failed(action)
            ReturnFailure
        }
      }
    results.foldLeft[ReturnStatus](ReturnSuccess){_ && _}
  }

  private def cleanInterruptedJobs(): Unit = {
    for {
      action <- workflow.getInterruptingJobs
      if action.isFinished
    } {
      action.result.get match {
        case ActionResult(ReturnSuccess, _, _) =>
          workflow.interrupted(action)
          feedback.successful(action)
        case res @ ActionResult(ReturnFailure, logPath, _) =>
          workflow.interrupted(action)
          feedback.interrupted(action)
      }
    }
  }

  /**
    * After an action has been completed, create new actions to be executed.
    * This method will be called every time an action is completed.
    * By overriding this method, new actions may be added during the workflow execution.
    *
    * By default, this method always returns an empty list.
    *
    * @param completedAction
    */
  protected def getNextActions(completedAction: A): Seq[A] = {
    Nil
  }

  /**
    * When an action is ready to start, check if the action execution should be delayed.
    * By overriding this method, you can implement a locking system on your action, for instance two prevent
    * two specific types of actions to run concurrently.
    * By default, this method always returns false.
    * @param action
    * @return
    */
  protected def delayAction(action: A): Boolean = {
    false
  }

  /**
    * @return a map of all jobs in the workflow associated with their status.
    */
  final def getAllJobStatus: Map[A, Status] = {
    workflow.getAllJobStatus
  }

  final def getTodoJobs: Seq[A] = {
    workflow.getTodoJobs
  }

  final def getRunningJobs: Seq[A] = {
    workflow.getRunningJobs
  }

  final def getSuccessfulJobs: Seq[A] = {
    workflow.getSuccessfulJobs
  }

  final def getFailedJobs: Seq[A] = {
    workflow.getFailedJobs
  }

  final def getSkippedJobs: Seq[A] = {
    workflow.getSkippedJobs
  }

  private def interruptRunningJob(job: A): Unit = {
    new Thread(
      new Runnable {
        override def run(): Unit = {
          job.interrupt()
        }
      }
    ).start()
  }

  private def interruptionLoop(): Unit = {
    workflow.getTodoJobs.foreach{workflow.skipped}
    workflow.getRunningJobs.foreach{
      a =>
        workflow.interrupting(a)
        feedback.interrupting(a)
        interruptRunningJob(a)
    }
    var continue = true
    var exceptionPrinted = false
    while(continue) {
      try {
        if (workflow.hasInterruptingJobs) {
          cleanInterruptedJobs()
        }
        else {
          continue = false
        }
      }
      catch {
        case e: Throwable if !exceptionPrinted =>
          exceptionPrinted = true
          e.printStackTrace()
        case e: InterruptedException => ()
      }
    }
  }

  final def run(startActions: Iterable[A]): ReturnStatus = {
    var ret: ReturnStatus = ReturnSuccess
    workflow.clear()
    startActions.foreach{workflow.todo}
    var continue = true
    while(continue) {
      if (workflow.hasTodoJobs || workflow.hasRunningJobs) {
          startTodoJobs()
          /* Careful here: because of lazy evaluation, cleanFinishedJobs() should come first */
          ret = cleanFinishedJobs() && ret
      }
      else {
        continue = false
      }
      if (workflow.hasRunningJobs) {
        workflow.getRunningJobs.foreach{feedback.running}
        try {
          Thread.sleep(ParallelActionRunner.SLEEP_TIME)
        }
        catch {
          case e: InterruptedException =>
            continue = false
            interrupted = true
        }
      }
      if(Thread.currentThread().isInterrupted){
        continue = false
        interrupted = true
      }
    }
    if(interrupted) {
      interruptionLoop()
    }
    ret
  }

  override def close(): Unit = {
    logger.debug("closing ParallelActionRunner")
    feedback.close()
  }

}

object ParallelActionRunner {
  val SLEEP_TIME = 100
}