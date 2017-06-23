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
import com.flaminem.flamy.Launcher.Options
import org.rogach.scallop.{CliOption, Scallop, ScallopConf}

/**
  * Created by fpin on 6/20/17.
  */
case class CliArgs(
  builder: Scallop,
  args: Seq[String],
  lastOption: Option[CliOption],
  lastOptionArgs: Seq[String],
  previousOptions: Seq[CliOption],
  trailArgs: Seq[String],
  lastWord: String
) {

  override def toString: String = {
    s"""CliArgs(
       |  args: $args
       |  lastOption: ${lastOption.map{_.name}.mkString(", ")}
       |  lastOptionArgs: ${lastOptionArgs.mkString(", ")}
       |  previousOptions: ${previousOptions.map{_.name}.mkString(", ")}
       |  trailArgs: ${trailArgs.mkString(", ")}
       |  lastWord: $lastWord
       |)
       |""".stripMargin
  }

}

object CliArgs {

  private def stringToCliOption(builder: Scallop, opt: String): Option[CliOption] = {
    if(opt.length==2 && opt(1) != '-') {
      builder.getOptionWithShortName(opt(1))
    }
    else {
      builder.opts.find{p => "--" + p.name == opt}
    }
  }

  def apply(args: Seq[String], lastWord: String): CliArgs = {
    val (argsBeforeLastOption, lastOptionAndArgs) =
      args.lastIndexWhere{s => s.startsWith("-")} match {
        case -1 => (args, Nil)
        case index => args.splitAt(index)
      }

    /* The exception is thrown when getting the builder, which is why we get both simultaneously */
    val (scallop: ScallopConf, builder: Scallop) =
      try {
        val s = Launcher.newLauncher(args).opts
        (s, s.subcommands.lastOption.getOrElse(s).builder)
      }
      catch {
        case e: org.rogach.scallop.exceptions.ScallopException =>
          val s = Launcher.newLauncher(argsBeforeLastOption).opts
          (s, s.subcommands.lastOption.getOrElse(s).builder)
      }

    val lastOption: Option[CliOption] = lastOptionAndArgs.headOption.flatMap{stringToCliOption(builder, _)}

    val previousOptions: Seq[CliOption] = argsBeforeLastOption.flatMap{stringToCliOption(builder, _)}

    val subCommands: List[String] = scallop.subcommands.map{_.toString}

    /** All the candidate arguments for the last option, except lastWord */
    val lastOptionArgs: Seq[String] =
      if(lastOptionAndArgs.isEmpty) {
        Nil
      }
      else {
        lastOptionAndArgs.tail
      }

    val trailArgs: Seq[String] =
      if(lastOption.isDefined) {
        Nil
      }
      else {
        subCommands.foldLeft(args){
          case (args, command) => args.dropWhile(_ != command).drop(1)
        }
      }

    new CliArgs(
      builder = builder,
      args = args,
      lastOption = lastOption,
      lastOptionArgs = lastOptionArgs,
      previousOptions = previousOptions,
      trailArgs = trailArgs,
      lastWord = lastWord
    )
  }


}