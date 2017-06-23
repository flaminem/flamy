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

import com.flaminem.flamy.exec.utils.ThreadPrintStream
import sun.misc.{Signal, SignalHandler}

/**
  * Handle interruptions signals
  */
object CommandInterruptSignalHandler extends SignalHandler {

  @volatile
  var interruptionLevel = 0

  val SIGINT = new Signal("INT")

  private val mainThread: Thread = Thread.currentThread()

  private var command: Option[Command] = None

  def setCommand(c: Command): Unit = {
    this.synchronized {
      command = Some(c)
    }
  }

  def unsetCommand(): Unit = {
    this.synchronized {
      command = None
    }
  }

  private def interrupt(): Unit = {
    ThreadPrintStream.out.println("Interrupting command, this may take some time...")
    command match {
      case None => mainThread.interrupt()
      case Some(c) => c.interrupt()
    }
    /* As long as the thread is not completely interrupted, we keep interrupting it
     * This is mostly because some libraries cancel the interruption while catching it
     */
    var continue = true
    while (continue) {
      Thread.sleep(1)
      this.synchronized {
        if (command.isDefined) {
          command.get.interrupt()
        }
        else {
          continue = false
        }
      }
    }
    interruptionLevel = 0
  }

  override def handle(signal: Signal): Unit = {
    interruptionLevel match {
      case 0 =>
        interruptionLevel += 1
        interrupt()
      case 1 =>
        interruptionLevel += 1
        ThreadPrintStream.out.println("Interrupting again will kill the JVM")
      case 2 => System.exit(1)
    }
  }
}
