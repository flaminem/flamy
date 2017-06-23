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

/**
  * Provides a set of methods to display the status of the executed actions.
  */
trait ActionFeedback extends AutoCloseable {

  /**
    * Gives the user feedback that the action has been started
    * @param action
    */
  def started(action: Action)

  /**
    * Gives the user feedback that the action is running
    * @param action
    */
  def running(action: Action)

  /**
    * Gives the user feedback that the action is successful
    * @param action
    */
  def successful(action: Action)

  /**
    * Gives the user feedback that the action has failed
    * @param action
    */
  def failed(action: Action)

  /**
    * Gives the user feedback that the action has been skipped
    * @param action
    */
  def skipped(action: Action)


  /**
    * Gives the user feedback that the action is interrupting
    * @param action
    */
  def interrupting(action: Action)

  /**
    * Gives the user feedback that the action has been interrupted
    * @param action
    */
  def interrupted(action: Action)

}


class SimpleActionFeedback extends ActionFeedback {

  override def started(action: Action): Unit = {
    System.out.println(f"Starting : ${action.name}")
  }

  override def running(action: Action): Unit = {
    /* nothing to do here */
  }

  override def successful(action: Action): Unit = {
    val message: String = action.message.map{ " (" + _ + ")" }.getOrElse("")
    System.out.println(f"Success : ${action.name} $message")
  }

  override def failed(action: Action): Unit = {
    val res = action.result.get
    System.out.println(s"Failure  : ${action.name}\n${res.fullErrorMessage}")
    action.message match {
      case Some(message) => System.out.println("\t" + message)
      case None => ()
    }
  }

  override def skipped(action: Action): Unit = {
    System.out.println(f"Skipping : ${action.name}")
  }

  override def interrupting(action: Action): Unit = {
    System.out.println(f"Interrupting : ${action.name}")
  }

  override def interrupted(action: Action): Unit = {
    System.out.println(f"Interrupted : ${action.name}")
  }

  override def close(): Unit = {

  }

}
