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

import com.flaminem.flamy.exec.utils.Workflow.Status

import scala.collection.mutable.ListBuffer

/**
  * Adds a layer to a SimpleWorkflow to keep the history of every status change.
  */
trait WorkflowHistory[T] extends Workflow[T]{

  private val history: ListBuffer[(T, Status)] = ListBuffer[(T, Status)]()

  override def todo(v: T): Unit = {
    history += v -> Workflow.Status.TODO
    super.todo(v)
  }

  override def running(v: T): Unit = {
    history += v -> Workflow.Status.RUNNING
    super.running(v)
  }

  override def successful(v: T): Unit = {
    history += v -> Workflow.Status.SUCCESSFUL
    super.successful(v)
  }

  override def failed(v: T): Unit = {
    history += v -> Workflow.Status.FAILED
    super.failed(v)
  }

  override def skipped(v: T): Unit = {
    history += v -> Workflow.Status.SKIPPED
    super.skipped(v)
  }

  override def interrupting(v: T): Unit = {
    history += v -> Workflow.Status.INTERRUPTING
    super.interrupting(v)
  }

  override def interrupted(v: T): Unit = {
    history += v -> Workflow.Status.INTERRUPTED
    super.interrupted(v)
  }

  def getHistory: Seq[(T, Status)] = history

}
