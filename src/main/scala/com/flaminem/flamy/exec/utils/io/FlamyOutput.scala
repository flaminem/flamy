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

package com.flaminem.flamy.exec.utils.io

import java.io.PrintStream

import com.flaminem.flamy.exec.utils.ThreadPrintStream
import com.flaminem.flamy.utils.macros.SealedValues
import org.fusesource.jansi.Ansi.Color._
import org.fusesource.jansi.Ansi._

/**
  * Provides method to send feedback messages to the user via the Standard Output,
  * allowing to configure the level of verbosity.
  *
  * The difference with log4j is that this class is intended for user-destined feedback, rather than program-monitoring logs.
  * This means that the output aims to be simpler to read for the user, and that flamy's verbosity should be simple to change.
  *
  * Use this for end-user-oriented feedback, and use log4j for more program-monitoring-oriented feedback.
  *
  */
object FlamyOutput {

  private val useColor: Boolean = {
    Option(System.console()).isDefined
  }

  @volatile
  private var initialized: Boolean = false

  def checkForInterruption(): Unit = {
    if(Thread.currentThread().isInterrupted){
      throw new InterruptedException()
    }
  }

  def init(): Unit = {
    this.synchronized {
      checkForInterruption()
      if(!initialized) {
        initialized = true
        ThreadPrintStream.init()
        jline.TerminalFactory.get().init()
      }
    }
  }

  def shutdown(): Unit = {
    this.synchronized {
      if(initialized){
        initialized = false
        jline.TerminalFactory.get().restore()
        ThreadPrintStream.shutdown()
      }
    }
  }

  sealed trait LogLevel {
    val name: String
    val intLevel: Int
    def shouldPrint(level: LogLevel): Boolean = {
      this.intLevel <= level.intLevel
    }
  }

  object LogLevel {

    case object DEBUG extends LogLevel {
      override val name: String = "DEBUG"
      override val intLevel: Int = 0
    }
    case object INFO extends LogLevel {
      override val name: String = "INFO"
      override val intLevel: Int = 1
    }
    case object WARN extends LogLevel {
      override val name: String = "WARN"
      override val intLevel: Int = 2
    }
    case object ERROR extends LogLevel {
      override val name: String = "ERROR"
      override val intLevel: Int = 3
    }
    case object SILENT extends LogLevel {
      override val name: String = "SILENT"
      override val intLevel: Int = 4
    }

    /* This line must stay after the value declaration or it will be empty */
    val values: Seq[LogLevel] = SealedValues.values[LogLevel]
    val logLevelNames: Seq[String] = values.map{_.name}

  }

  import LogLevel._

  def setLogLevel(logLevelName: String): Unit = {
    values.find{_.name == logLevelName} match {
      case Some(level) => logLevel = level
      case None => ()
    }
  }

  /**
    * true if the current level allows to print the specified level
    *
    * @param level
    */
  def shouldPrint(level: LogLevel): Boolean ={
    logLevel.shouldPrint(level)
  }

  @volatile var logLevel: LogLevel = INFO

  object out extends Printer() {
    override def stream: PrintStream = System.out
  }

  object err extends Printer() {
    override def stream: PrintStream = System.err
  }

  trait Printer {

    protected def stream: PrintStream

    def flush(): Unit = {
      stream.flush()
    }

    def println(s: => String): Unit = {
      stream.println(s)
    }

    def println(s: => String, color: Color): Unit = {
      val message: String =
        if(useColor){
          ansi.fg(color).a(s).fg(DEFAULT).toString
        }
        else{
          s
        }
      println(message)
    }

    def success(s: => String): Unit = {
      println(s, GREEN)
    }

    def failure(s: => String): Unit = {
      println(s, RED)
    }

    def debug(s: => String): Unit = {
      if(shouldPrint(DEBUG)){
        stream.println(s"DEBUG: $s")
      }
    }

    def debug(e: Throwable): Unit = {
      if(shouldPrint(DEBUG)){
        e.printStackTrace(stream)
      }
    }

    def info(s: => String): Unit = {
      if(shouldPrint(INFO)){
        stream.println(s"INFO: $s")
      }
    }

    def warn(s: => String): Unit = {
      if(shouldPrint(WARN)){
        println(s"WARNING: $s", YELLOW)
      }
    }

    def error(s: => String): Unit = {
      if(shouldPrint(ERROR)){
        println(s"ERROR: $s", RED)
      }
    }

    def error(e: Throwable): Unit = {
      if(shouldPrint(ERROR)){
        e.printStackTrace(stream)
      }
    }

  }


}
