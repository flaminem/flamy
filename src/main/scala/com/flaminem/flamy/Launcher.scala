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

package com.flaminem.flamy

import java.io.File

import com.flaminem.flamy.commands.Exit
import com.flaminem.flamy.commands.utils.{FlamyScallopConf, FlamySubcommand, ScallopUtils}
import com.flaminem.flamy.conf.hive.ModelHiveContext
import com.flaminem.flamy.conf._
import com.flaminem.flamy.exec.files.FileRunner
import com.flaminem.flamy.exec.utils._
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.utils.CliUtils
import com.flaminem.flamy.utils.logging.Logging
import org.rogach.scallop._
import org.rogach.scallop.exceptions.{Help, ScallopException, ScallopResult, Version}

import scala.language.reflectiveCalls
import scala.util.control.NonFatal

/**
  * The application launcher for flamy.
  * By updating the newLauncher variable, it is possible to extend flamy with new proprietary commands.
  */
object Launcher {

  /* By updating this variable with another object extending the Launcher class,
   * it is possible to extend flamy's syntax with new proprietary commands.
   */
  var newLauncher : (Seq[String]) => Launcher = {
    args: Seq[String] => new Launcher(args)
  }

  /*
    * Only this method is allowed to call System.exit
    */
  def main(args: Array[String]): Unit = {
    FlamyOutput.init()
    val returnStatus =
      try {
        new Launcher(args, withShell = true).launch()
      }
      finally{
        FlamyOutput.shutdown()
      }
    System.exit(returnStatus.exitCode)
  }

  def launch(args: Seq[String]): ReturnStatus = {
    new Launcher(args).launch()
  }

  class Options(args: Seq[String]) extends ScallopConf(args) with FlamyScallopConf {
    val helpCommand =
      new Subcommand("help") {
        banner("Print help for this program")
        val subCommands: ScallopOption[List[String]] =
          trailArg[List[String]](default = Some(Nil), name = "command", required = false)
      }

    val helpOption: ScallopOption[Boolean] =
      opt(name = "help", short = 'h', default = Some(false), descr = "print help", required = false, hidden = true)

    val version =
      new Subcommand("version") {
        banner("Print version information of this program")
      }

    val versionOption: ScallopOption[Boolean] =
      opt(name = "version", noshort = true, default = Some(false), descr = "print version number", required = false, hidden = true)

    val variables: ScallopOption[Variables] =
      opt[Variables](
        name = "variables",
        default = Some(Variables()),
        descr = "Specifies the variables in the command line. This may override definitions from the variables file."
      )(Variables.scallopConverter)

    val configFile: ScallopOption[String] =
      opt[String](name = "config-file", noshort=true, argName="PATH", descr="Use the configuration file at the specified PATH instead of the one in the conf folder.")

    val conf: Map[String, String] =
      propsLong[String](name = "conf", descr = "Specifies flamy configuration properties. This may override definitions from the configuration file.")

    lazy val globalOptions: FlamyGlobalOptions = new FlamyGlobalOptions(variables(), conf, configFile.get)

    var optError: Option[ReturnStatus] = None
    var optException: Option[Exception] = None

    /** Print help message (with version, banner, option usage and footer) to stdout. */
    def printHelp(subCommand: String): Unit = {
      subCommand match {
        case "" =>
          builder.vers.foreach{println}
          builder.bann.foreach{println}
          println("Global Options:")
          val globalOptionsDesc: List[(String, String, Option[String])] =
            builder.opts.flatMap {
              case o: LongNamedPropertyOption => List((s"--${o.name} key=value [key=value]...", o.descr, None))
              case o => o.helpInfo(o.shortNames)
            }
          println(Formatter.format(globalOptionsDesc.map{Option(_)}))
          println("Commands:")
          ScallopUtils.printSubCommands(builder)
          builder.foot.foreach{println}
        case subName =>
          builder.findSubbuilder(subName) match {
            case None =>
              throw new FlamyException(s"no such command found : ${subName.replace("\u0000"," ")}")
            case Some(subBuilder) =>
              ScallopUtils.printHelp(subBuilder, subName)
          }

      }
    }

    override def onError(e: Throwable): Unit = {
      e match {
        case r: ScallopResult =>
          r match {
            case Help("") =>
              this.printHelp(this.builder.getSubcommandNames.mkString("\u0000"))
              optError = Some(ReturnSuccess)
            case Help(subName) =>
              this.printHelp(subName)
              optError = Some(ReturnSuccess)
            case Version =>
              builder.vers.foreach(println)
              optError = Some(ReturnSuccess)
            case ScallopException(message) =>
              optException = Some(new FlamyException(s"[$printedName] : $message"))
              optError = Some(ReturnFailure)
          }
        case _ => throw e
      }
    }

    version(f"Flamy ${Flamy.version}")
    banner( s"""Usage: ${Flamy.name} <GLOBAL_OPTIONS> <COMMAND>""".stripMargin)
    footer("\n")
    //    verify()
  }

  class Commands(args: Seq[String]) extends Options(args) {

    val show = new commands.Show
    val drop = new commands.Drop
    val diff = new commands.Diff
    val describe = new commands.Describe
    val check = new commands.Check
    val run = new commands.Run
    val push = new commands.Push
    val repair = new commands.Repair
    val count = new commands.Count
    val waitForPartition = new commands.WaitForPartition
    val gatherInfo = new commands.GatherInfo

    /* This feature is not complete yet */
//    val test = new commands.Test
  }

  class CommandsOutsideShell(args: Seq[String]) extends Commands(args) {
    val shell = new commands.Shell
  }

  class CommandsInShell(args: Seq[String]) extends Commands(args) {
    val exit = new commands.Exit
  }

}


class Launcher protected(args: Seq[String], withShell: Boolean = false) extends Logging {

  import Launcher._

  lazy val opts: Options = {
    if(withShell) {
      new CommandsOutsideShell(args)
    }
    else {
      new CommandsInShell(args)
    }
  }

  /*
   * For some very obscure reason, if you replace this with an Option[FileRunner#Stats],
   * you get one of the most ugly java error in history (goto + bytecode...)
   */
  //noinspection ScalaStyle
  private[flamy] var stats: FileRunner#Stats = null

  /*
   * This method is used in tests and shall never call System.exit
   */
  def launch(globalOptions: FlamyGlobalOptions = new FlamyGlobalOptions()): ReturnStatus = {
    /* This is necessary to avoid collisions with other running instances of flamy */
    try {
      FlamyContext.reset()
      ModelHiveContext.reset()
      unsafeLaunch(globalOptions)
    }
    catch {
      case NonFatal(e) =>
        handleException(e)
        ReturnFailure
    }
    finally {
      new File("derby.log").delete
      if (stats != null) {
        FlamyOutput.out.info(stats.format())
      }
    }
  }

  /**
    *
    * @param rootGlobalOptions when running a shell, we want to preserve the options defined in the shell starting command
    * @return
    */
  private def unsafeLaunch(rootGlobalOptions: FlamyGlobalOptions): ReturnStatus = {
    if(opts.optException.isDefined) {
      handleException(opts.optException.get)
    }

    /* This "if" cannot be moved inside the "match case" because opts.subcommands is not defined when optError is defined */
    if(opts.optError.isDefined) {
      opts.optError.get
    }
    else {
      opts.subcommands match {
        case Nil =>
          opts.printHelp("")
          ReturnFailure

        case (command@opts.helpCommand) :: Nil =>
          opts.printHelp(command.subCommands().mkString("\u0000"))
          ReturnSuccess

        case (command@opts.version) :: Nil =>
          opts.builder.vers.foreach{println}
          ReturnSuccess

        case (command: FlamySubcommand) :: subCommands =>
          val res: ReturnStatus = command.doCommand(rootGlobalOptions.overrideWith(opts.globalOptions), subCommands)
          stats = command.stats.orNull
          res

        case commands =>
          throw new IllegalArgumentException("Unknown Commands: " + commands.mkString(", "))
      }
    }
  }

  def handleException(e: Throwable): Unit = {
    def getMessage(e: Throwable): String = {
      e match {
        case FlamyException(Some(message), Some(cause)) =>
          e.getMessage + ":\n" + getMessage(cause)
        case FlamyException(None, Some(cause)) =>
          getMessage(cause)
        case FlamyException(Some(message), None) =>
          e.getMessage
        case _: Throwable =>
          FlamyOutput.out.error(e)
          Option(e.getMessage).getOrElse("")
      }
    }
    FlamyOutput.out.error(getMessage(e))
    FlamyOutput.out.debug(e)
  }

}