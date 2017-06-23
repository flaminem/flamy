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

import java.io.{File, PrintStream}

import com.flaminem.flamy.conf.FlamyGlobalContext
import com.flaminem.flamy.exec.utils.Workflow.Status
import com.flaminem.flamy.utils.io.ConcurrentFilePrintStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

trait ActionLogging[A <: Action] extends ParallelActionRunner[A]{

  private val LOG_PATH: String = "/../logs.csv"
  private val log: PrintStream = new ConcurrentFilePrintStream(new File(FlamyGlobalContext.getRunDir + LOG_PATH), FileSystem.get(new Configuration), true)

  private def log(v: Action, running: Status) {
    val row = Seq(System.currentTimeMillis, context.getProject, context.getEnvironment, context.dryRun, v.name, running)
    log.println(row.mkString("\t"))
  }

  override protected val workflow = new Workflow[A] with WorkflowLogging

  trait WorkflowLogging extends Workflow[A] {

    override def running(v: A) {
      log(v, Status.RUNNING)
      super.running(v)
    }

    override def successful(v: A) {
      log(v, Status.SUCCESSFUL)
      super.successful(v)
    }

    override def failed(v: A) {
      log(v, Status.FAILED)
      super.failed(v)
    }

  }

}

