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

package com.flaminem.flamy.exec.shell

import com.flaminem.flamy.Launcher
import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.exceptions.FlamyException

import scala.util.control.NonFatal

/**
  * Created by fpin on 2/16/17.
  */
class Command(launcher: Launcher, context: FlamyContext) extends Runnable {

  private var interrupted = false

  val thread = new Thread(this, "shell command")

  def interrupt(): Unit = {
    interrupted = true
    thread.interrupt()
  }

  override def run(): Unit = {
    try {
      launcher.launch(context.globalOptions)
    }
    catch {
      case e: Throwable if interrupted => println("Command interrupted by user")
      case NonFatal(e) => throw new FlamyException("Command failed:", e)
    }
  }

}
