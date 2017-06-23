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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

/**
 * TODO: unit-test this class
 */
object Workflow {
  sealed trait Status {
    val name: String
  }
  object Status {
    case object TODO extends Status {
      override val name: String = "TODO"
    }
    case object RUNNING extends Status {
      override val name: String = "RUNNING"
    }
    case object SUCCESSFUL extends Status {
      override val name: String = "SUCCESSFUL"
    }
    case object FAILED extends Status {
      override val name: String = "FAILED"
    }
    case object SKIPPED extends Status {
      override val name: String = "SKIPPED"
    }
    case object INTERRUPTING extends Status {
      override val name: String = "INTERRUPTING"
    }
    case object INTERRUPTED extends Status {
      override val name: String = "INTERRUPTED"
    }

  }
}

class Workflow[T] {
  import Workflow._

  private val jobStatus: ConcurrentHashMap[T, Status] = new ConcurrentHashMap[T, Status]

  private def getStatus(v: T): Status = {
    if (!jobStatus.containsKey(v)) {
      throw new RuntimeException("Error: job not found.")
    }
    jobStatus.get(v)
  }

  def getAllJobStatus: Map[T, Status] = {
    jobStatus.toMap
  }

  def isJobTodo(v: T): Boolean = getStatus(v) == Status.TODO

  def isJobRunning(v: T): Boolean = getStatus(v) == Status.RUNNING

  def isJobSuccessful(v: T): Boolean = getStatus(v) == Status.SUCCESSFUL

  def isJobFailed(v: T): Boolean = getStatus(v) == Status.FAILED

  def isJobSkipped(v: T): Boolean = getStatus(v) == Status.SKIPPED

  def todo(v: T) {
    if (jobStatus.putIfAbsent(v, Status.TODO) != null) {
      throw new RuntimeException("Job " + v + " is already present in the workflow")
    }
  }

  def running(v: T) {
    if (!jobStatus.replace(v, Status.TODO, Status.RUNNING)) {
      throw new RuntimeException("Job " + v + " is not todo")
    }
  }

  def successful(v: T) {
    if (!jobStatus.replace(v, Status.RUNNING, Status.SUCCESSFUL)) {
      throw new RuntimeException("Job " + v + " is not running")
    }
  }

  def failed(v: T) {
    if (!jobStatus.replace(v, Status.RUNNING, Status.FAILED)) {
      throw new RuntimeException("Job " + v + " is not running")
    }
  }

  def skipped(v: T) {
    if (!jobStatus.replace(v, Status.TODO, Status.SKIPPED)) {
      throw new RuntimeException("Job " + v + " is not ready")
    }
  }

  def interrupting(v: T) {
    if (!jobStatus.replace(v, Status.RUNNING, Status.INTERRUPTING)) {
      throw new RuntimeException("Job " + v + " is not running")
    }
  }

  def interrupted(v: T) {
    if (!jobStatus.replace(v, Status.INTERRUPTING, Status.INTERRUPTED)) {
      throw new RuntimeException("Job " + v + " is not interrupting")
    }
  }

  def getJobsWithStatus(status: Workflow.Status): Seq[T] = {
    jobStatus.entrySet().toSeq.filter{_.getValue==status}.map{_.getKey}
  }

  def getJobsWithoutStatus(status: Workflow.Status): Seq[T] = {
    jobStatus.entrySet().toSeq.filter{_.getValue!=status}.map{_.getKey}
  }

  def getTodoJobs: Seq[T] = getJobsWithStatus(Status.TODO)

  def getRunningJobs: Seq[T] = getJobsWithStatus(Status.RUNNING)

  def getSuccessfulJobs: Seq[T] = getJobsWithStatus(Status.SUCCESSFUL)

  def getFailedJobs: Seq[T] = getJobsWithStatus(Status.FAILED)

  def getSkippedJobs: Seq[T] = getJobsWithStatus(Status.SKIPPED)

  def getInterruptingJobs: Seq[T] = getJobsWithStatus(Status.INTERRUPTING)

  def getInterruptedJobs: Seq[T] = getJobsWithStatus(Status.INTERRUPTED)

  private def hasJobsWithStatus(s: Workflow.Status): Boolean = jobStatus.containsValue(s)

  def hasTodoJobs: Boolean = hasJobsWithStatus(Status.TODO)

  def hasRunningJobs: Boolean = hasJobsWithStatus(Status.RUNNING)

  def hasSuccessfulJobs: Boolean = hasJobsWithStatus(Status.SUCCESSFUL)

  def hasFailedJobs: Boolean = hasJobsWithStatus(Status.FAILED)

  def hasSkippedJobs: Boolean = hasJobsWithStatus(Status.SKIPPED)

  def hasInterruptingJobs: Boolean = hasJobsWithStatus(Status.INTERRUPTING)

  def hasInterruptedJobs: Boolean = hasJobsWithStatus(Status.INTERRUPTED)

  def clear(): Unit = jobStatus.clear()

  override def toString: String = "Workflow [jobs=" + jobStatus + "]"
}
