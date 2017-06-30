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

import java.io.File
import java.nio.file.{Path, Paths}

import com.flaminem.flamy.Launcher
import com.flaminem.flamy.commands.Exit
import com.flaminem.flamy.conf.{Flamy, FlamyContext, SystemContext}
import com.flaminem.flamy.utils.CliUtils
import com.flaminem.flamy.utils.logging.Logging
import jline.UnixTerminal
import jline.console.history.{FileHistory, History, MemoryHistory}
import jline.console.{ConsoleReader, UserInterruptException}
import sun.misc.Signal

import scala.util.control.NonFatal

/**
  * Created by fpin on 2/16/17.
  */
class ShellCLI extends Logging {

  private val consoleReader: ConsoleReader = new ConsoleReader()
  consoleReader.setPrompt(Flamy.name + "> ")

  private val handler = new CandidateListCompletionHandler()
  consoleReader.setCompletionHandler(handler)
  consoleReader.setHistoryEnabled(true)

  private val history: History = {
    val historyPath: Path = Paths.get(SystemContext.userHome,s".${Flamy.name}_history")
    try {
      logger.debug(s"Opening history file at $historyPath")
      new FileHistory(new File(historyPath.toUri))
    }
    catch {
      case NonFatal(e) =>
        val message = Option(e.getMessage).map{" : " + _}.getOrElse("")
        logger.warn(s"Could not open history file at $historyPath$message")
        new MemoryHistory()
    }
  }
  consoleReader.setHistory(history)

  consoleReader.setCopyPasteDetection(true)
  consoleReader.setHandleUserInterrupt(true)

  private val intr: Option[String] = {
    consoleReader.getTerminal match {
      case terminal: UnixTerminal =>
        Some(terminal.getSettings.getPropertyAsString("intr"))
      case _ => None
    }
  }

  /**
    * The UnixTerminal.disableInterruptCharacter is called twice, it is impossible to re-enable it...
    * This method provides a workaround.
    */
  private def unixTerminalInterruptCharacterBugfix() = {
    consoleReader.getTerminal match {
      case terminal: UnixTerminal if intr.isDefined =>
        terminal.getSettings.set("intr", intr.get)
      case _ => ()
    }
  }

  /*
   * We added a workaround for a bug happening with the ua_parser UDF:
   * Somehow Hive or Java seems to lose the reference to the resource file "/ua_parser/regexes.yaml"
   * when running for the second time, which causes this error:
   * YAMLException java.io.IOException: Stream closed
   * Executing the command in a new thread seems to prevent it from happening somehow.
   */
  private def readLine(line: String, rootContext: FlamyContext): Unit = {
    import CommandInterruptSignalHandler.SIGINT
    val previousHandler = Signal.handle(SIGINT, CommandInterruptSignalHandler)
    try {
      val args: Array[String] = CliUtils.split(line).filter{_.nonEmpty}.toArray
      val command: Command = new Command(Launcher.newLauncher(args), rootContext)
      val thread: Thread = command.thread
      logger.debug("starting command")
      CommandInterruptSignalHandler.setCommand(command)
      thread.start()
      thread.join()
      logger.debug("command finished")
    }
    catch {
      case NonFatal(e) => println(e.getMessage)
    }
    finally{
      Signal.handle(SIGINT, previousHandler)
      CommandInterruptSignalHandler.unsetCommand()
    }
  }

  def mainLoop(rootContext: FlamyContext): Unit = {
    val completer = new ShellCompleter(rootContext)
    consoleReader.addCompleter(completer)
    consoleReader.setCompletionHandler(completer.handler)
    var continue = true ;
    while(continue){
      try{
        val line = Option(consoleReader.readLine())
        unixTerminalInterruptCharacterBugfix()
        if(line.isEmpty) {
          println()
          continue = false
        }
        else if(line.get.isEmpty){
          /* this prevent from printing the help when the line is empty */
        }
        else {
          readLine(line.get, rootContext)
        }
      }
      catch {
        case e: InterruptedException => ()
        case e: UserInterruptException => ()
      }
    }
    history match {
      case h: FileHistory => h.flush()
      case _ => ()
    }
  }

}