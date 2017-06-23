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

import java.io.{File, IOException}
import java.net.{URI, URISyntaxException}

import com.flaminem.flamy.exec.utils.io.FlamyOutput
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 * An ActionRunner is used to run an [[Action]], either sequentially or in parallel.
 */
object ActionRunner {

  ThreadPrintStream.init()

  private val fs: FileSystem = {
    try {
      FileSystem.get(new URI("file:///"), new Configuration)
    }
    catch {
      case e: IOException => throw new RuntimeException(e)
      case e: URISyntaxException => throw new RuntimeException(e)
    }
  }

  /**
    * Starts the given action in a new thread, without waiting for it to finish.
    * @param action
    */
  def runInParallel(action: Action): Unit = {
    val thread: Thread = new Thread(new StreamText(action, fs), action.name)
    thread.start()
  }

  def run(action: Action): ActionResult = {
    new StreamText(action, fs).run()
    action.result.get
  }

  def run(action: Action, parallelize: Boolean): Unit = {
    if(parallelize) {
      runInParallel(action)
    }
    else {
      run(action)
    }
  }

  def getActionLogPath(action: Action): String = new File(action.fullLogPath).getAbsolutePath

}


class ActionRunner(silentOnSuccess: Boolean = true, silentOnFailure: Boolean = false){

  class Stats{
    private[utils] var successCount: Int = 0
    private[utils] var failCount: Int = 0

    def getSuccessCount: Int = successCount
    def getFailCount: Int = failCount

    def clear(): Unit = {
      stats.successCount = 0
      stats.failCount = 0
    }

    /**
      * Format a string to be printed for the user, indicated the number of Files {pastAction} and the number of Failures.
      * @param pastAction A verb conjugated to the past. For example: "ran", "analyzed"
      * @return
      */
    def format(pastAction: String = "ran"): String = {
      s"Files $pastAction: ${successCount + failCount}    Failures: $failCount"
    }

  }

  private val stats = new Stats

  def getStats: Stats = stats

  private def succeed(action: Action): Unit = {
    stats.successCount += 1
    if(!silentOnSuccess){
      FlamyOutput.err.success("ok: " + action.name)
      FlamyOutput.err.flush()
    }
  }

  private def fail(action: Action, result: ActionResult): Unit = {
    stats.failCount += 1
    if(!silentOnFailure) {
      FlamyOutput.err.failure("not ok: " + action.name)
      FlamyOutput.err.println(result.fullErrorMessage)
      FlamyOutput.err.flush()
    }
  }

  def run(actions: Iterable[Action]): Unit = {
    for (action <- actions) {
      val result = ActionRunner.run(action)
      /* We wait for 1ms to let the InterruptSignalHandler set thread.isInterrupted to true again, in case of interruption */
      Thread.sleep(1)
      result match {
        case _ if result.returnStatus.isSuccess => succeed(action)
        case _ if Thread.currentThread().isInterrupted =>
          /* If the thread was interrupted, we consider that the failure was caused by the interruption */
          throw new InterruptedException()
        case _ =>
          fail(action,result)
      }
    }
  }

}