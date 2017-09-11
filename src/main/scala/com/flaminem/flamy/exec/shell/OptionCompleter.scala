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

import com.flaminem.flamy.conf.{Environment, FlamyContext}
import com.flaminem.flamy.exec.hive.HiveTableFetcher
import com.flaminem.flamy.exec.shell.ShellCompleter.Candidates
import com.flaminem.flamy.model.ItemFilter
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import com.flaminem.flamy.utils.AutoClose
import com.flaminem.flamy.utils.logging.Logging
import org.rogach.scallop.{ArgType, CliOption, Scallop, TrailingArgsOption}

/**
  * Created by fpin on 5/31/17.
  */
class OptionCompleter(handler: CandidateListCompletionHandler, rootContext: FlamyContext) extends Logging {

  private def completeItemNames(lastWord: String, trailArgs: Seq[String]): Candidates = {
    completeItemNames(lastWord, trailArgs, "")
  }

  private def completeTableNames(lastWord: String, trailArgs: Seq[String]): Candidates = {
    completeItemNames(lastWord, trailArgs, ".")
  }

  private def completeItemNames(lastWord: String, trailArgs: Seq[String], schemaSuffix: String): Candidates = {
    val itemFilter: ItemFilter = ItemFilter(trailArgs.flatMap{ItemName.tryParse(_).toOption}, acceptIfEmpty = false)
    for {
      fetcher <- AutoClose(HiveTableFetcher(rootContext))
    } yield {
      /* We consider three cases depending on lastWord */
      lastWord.split("[.]", 2).toList match {
        case Nil => /* case 1: "" */
          /* This is quite ugly, but we don't wand to append a space after completing the schema*/
          handler.setPrintSpaceAfterFullCompletion(false)
          Candidates(
            fetcher.listSchemaNames.map{_.name}.filterNot{trailArgs.contains}.map{_ + schemaSuffix}.toSeq
          )
        case s::Nil => /* case 2: "schema" */
          handler.setPrintSpaceAfterFullCompletion(false)
          Candidates(
            fetcher.listSchemaNames.map{_.name}.filterNot{trailArgs.contains}.map{_ + schemaSuffix}.filter{_.startsWith(s)}.toSeq
          )
        case s::t::Nil if SchemaName.parse(s).isDefined => /* case 3: "schema.something" */
          Candidates(
            fetcher.listTablesNamesInSchema(SchemaName(s))
              .filter{_.name.startsWith(t)}
              .filterNot{itemFilter(_)}
              .map{_.name}
              .toSeq,
            shift = s.length + 1
          )
        case _ =>
          Candidates(Nil)
      }
    }
  }

  private def completeTrailArgs(builder: Scallop, lastWord: String, trailArgs: Seq[String]): Candidates = {
    builder.opts.collect {
      case opt: TrailingArgsOption if opt.converter == ItemName.scallopConverterList  => completeItemNames(lastWord, trailArgs)
      case opt: TrailingArgsOption if opt.converter == TableName.scallopConverter     => completeTableNames(lastWord, trailArgs)
      case opt: TrailingArgsOption if opt.converter == TableName.scallopConverterList => completeTableNames(lastWord, trailArgs)
    }.headOption.getOrElse(Candidates(Nil))
  }

  private def completeEnvironment(lastWord: String): Candidates = {
    Candidates(rootContext.getPossibleEnvironments.map{_.name}.filter{_.startsWith(lastWord)})
  }

  private def completeOptionArgs(cliArgs: CliArgs): Candidates = {
    val optionArgs: Seq[String] = cliArgs.lastOptionArgs
    cliArgs.lastOption.get.converter match {
      case x if x == Environment.scallopConverter && optionArgs.isEmpty => completeEnvironment(cliArgs.lastWord)
      case x if x == ItemName.scallopConverterList  => completeItemNames(cliArgs.lastWord, optionArgs)
      case x if x == TableName.scallopConverter && optionArgs.isEmpty => completeTableNames(cliArgs.lastWord, optionArgs)
      case x if x == TableName.scallopConverterList => completeTableNames(cliArgs.lastWord, optionArgs)
      case x  =>
        val afterOptionArgs =
          x.argType match {
            case ArgType.FLAG => cliArgs.lastOptionArgs
            case ArgType.SINGLE => cliArgs.lastOptionArgs.drop(1)
            case ArgType.LIST => Nil
          }
        /* Option args are already completed, so we continue as if no option was provided */
        complete(
          cliArgs.copy(
            lastOption = None,
            lastOptionArgs = Nil,
            previousOptions = cliArgs.previousOptions ++ cliArgs.lastOption,
            trailArgs = afterOptionArgs
          )
        )
    }
  }

  private def completeOption(builder: Scallop, lastWord: String, previousOptions: Seq[CliOption]): Candidates = {
    val previousOptionNames = previousOptions.flatMap { o => ("--" + o.name) +: o.shortNames.map {"-" + _} }.toSet
    Candidates(getOptions(builder).filter{_.startsWith(lastWord)}.filterNot{previousOptionNames.contains})
  }

  private def completeSubCommands(builder: Scallop, lastWord: String): Option[Candidates] = {
    val subCommands: Seq[String]  = getSubCommands(builder)
    if(subCommands.isEmpty) {
      None
    }
    else {
      Some(Candidates(subCommands.filter{_.startsWith(lastWord)}))
    }
  }

  private def getOptions(builder: Scallop): Seq[String] = {
    val shortOpts: Seq[String] =
      for {
        opt <- builder.opts
        name <- opt.shortNames
      } yield {
        "-" + name
      }
    val longOpts: List[String] =
      for {
        opt <- builder.opts
        name <- opt.longNames
      } yield {
        "--" + name
      }
    shortOpts ++ longOpts
  }

  private def getSubCommands(builder: Scallop): Seq[String] = {
    for {
      (name, sub: Scallop) <- builder.subbuilders
      if !sub.shortSubcommandsHelp
    } yield {
      name
    }
  }

  def complete(cliArgs: CliArgs): Candidates = {
    logger.debug(s"cliArgs: $cliArgs")
    lazy val subCommands: Option[Candidates] = completeSubCommands(cliArgs.builder, cliArgs.lastWord)
    if(cliArgs.lastOption.isDefined) {
      completeOptionArgs(cliArgs)
    }
    else if(cliArgs.lastWord.startsWith("-")) {
      completeOption(cliArgs.builder, cliArgs.lastWord, cliArgs.previousOptions)
    }
    else if(subCommands.isDefined){
      subCommands.get
    }
    else {
      completeTrailArgs(cliArgs.builder, cliArgs.lastWord, cliArgs.trailArgs)
    }
  }


}
