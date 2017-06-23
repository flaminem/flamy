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

import com.flaminem.flamy.conf.FlamyGlobalContext

import scala.util.control.NonFatal

/**
 * Created by fpin on 5/25/15.
 */
trait Action {

  private var executionThread: Option[Thread] = None

  /**
    * The name of this action. The same name will be given to the thread if it is run in parallel
    */
  val name: String

  /**
    * Path in the run directory where the logs of this action should be written
    */
  val logPath: String


  /**
    * Override this method to print a message when the action is finished.
    * @return
    */
  def message: Option[String] = None


  /**
    * Implement this method to define what this action does.
    * This is the method that will be called when the action is run by the [[ActionRunner]].
    * @throws java.lang.Exception
    */
  @throws(classOf[Exception])
  def run()

  /**
    * Full path where the logs of this action will be written
    */
  /* This needs to be lazy as logPath is abstract, it will instantiated after this trait */
  lazy val fullLogPath: String = FlamyGlobalContext.getLogFolder + "/" + logPath

  @volatile
  private var _isFinished: Boolean = false

  /**
    * True if this action has finished executing, whether it succeeded or failed
    */
  def isFinished: Boolean = _isFinished

  private var _result: Option[ActionResult] = None

  /**
    * The result of this action. Only defined when the action is finished.
    * @return
    */
  def result: Option[ActionResult] = _result

  def interrupt(): Unit = {
    executionThread.foreach {
      thread => thread.interrupt()
    }
  }

  @throws(classOf[Exception])
  private[utils] def execute(): Unit = {
    try {
      executionThread = Some(Thread.currentThread())
      run()
      this._result = Some(new ActionResult(ReturnSuccess, fullLogPath, None))
    }
    catch {
      case e: InterruptedException =>
        System.err.println(s"action ($name) has been interrupted")
        this._result = Some(new ActionResult(ReturnFailure, fullLogPath, Some(e)))
      case NonFatal(e) =>
        System.err.println(s"action ($name) failed.\nException : ")
        e.printStackTrace()
        this._result = Some(new ActionResult(ReturnFailure, fullLogPath, Some(e)))
    }
    finally {
      _isFinished = true
    }
  }

}

object Action {

  implicit def ordering[A <: Action]: Ordering[A] = {
    new Ordering[A] {
      override def compare(x: A, y: A): Int = {
        x.name.compare(y.name)
      }
    }
  }

}
