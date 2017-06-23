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

import com.flaminem.flamy.utils.io.MultilineWriter

import scala.collection.mutable

/**
  * Created by fpin on 1/9/17.
  */
class MultilineActionFeedback extends ActionFeedback {

  val writer = MultilineWriter(withMemory = true, withDebug = false)

  val indexer = new MultilineActionFeedback.IncrementalIndexer[Action]()

  /**
    * Prints the given text at the line associated with the given action
    * @param action
    * @param text
    */
  private def printForAction(action: Action, text: String): Unit = {
    writer.printLine(indexer(action), text)
  }

  /**
    * Gives the user feedback that the action has been started
    *
    * @param action
    */
  override def started(action: Action): Unit = {
    val message: String = action.message.map{ " (" + _ + ")" }.getOrElse("")
    printForAction(action, f"${action.name}\nStarted : $message\n")
  }

  /**
    * Gives the user feedback that the action is running
    *
    * @param action
    */
  override def running(action: Action): Unit = {
    started(action)
  }

  /**
    * Gives the user feedback that the action is successful
    *
    * @param action
    */
  override def successful(action: Action): Unit = {
    val message: String = action.message.map{ " (" + _ + ")" }.getOrElse("")
    printForAction(action, f"${action.name}\nSuccess: $message")
  }

  /**
    * Gives the user feedback that the action has failed
    *
    * @param action
    */
  override def failed(action: Action): Unit = {
    val res = action.result.get
    val w = writer.terminalWidth
    val name = writer.truncateString(s"${action.name}", w)
    val errorMessage =
      if(res.errorMessage.contains("\n")){
        val line = res.errorMessage.takeWhile{_ != '\n'}
        writer.truncateString(s"Failure: $line", w)
      }
      else {
        writer.truncateString(s"Failure: ${res.errorMessage}", w)
      }
    val logPathMessage = writer.truncateString(res.logPathMessage, w)
    printForAction(action, s"$name\n$errorMessage\n$logPathMessage")
  }

  override def skipped(action: Action): Unit = {
    printForAction(action, f"${action.name}\nSkipped\n")
  }

  override def interrupting(action: Action): Unit = {
    printForAction(action, f"${action.name}\nInterrupting")
  }

  override def interrupted(action: Action): Unit = {
    printForAction(action, f"${action.name}\nInterrupted")
  }

  override def close(): Unit = {
    writer.close()
  }

}

object MultilineActionFeedback {

  /**
    * Small utility to associate a set of items with an incremental id.
    * @tparam A
    */
  class IncrementalIndexer[A] {
    val indices: mutable.HashMap[A, Int] = mutable.HashMap[A, Int]()

    def apply(action: A): Int = {
      indices.getOrElseUpdate(action, indices.size)
    }
  }

}
