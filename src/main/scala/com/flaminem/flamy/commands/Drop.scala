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

package com.flaminem.flamy.commands

import com.flaminem.flamy.commands.utils.FlamySubcommand
import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyGlobalOptions}
import com.flaminem.flamy.exec.FlamyRunner
import com.flaminem.flamy.exec.actions.{DropSchemaAction, DropTableAction}
import com.flaminem.flamy.exec.hive.HiveTableFetcher
import com.flaminem.flamy.exec.utils.{Action, _}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model.{ItemFilter, TableInfo}
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

class Drop extends Subcommand("drop") with FlamySubcommand {

  val schemas: Subcommand = new Subcommand("schemas") with FlamySubcommand {
    banner("Drop the specified schemas on the specified environment")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to run on", required = true, noshort = true)
    val dryRun: ScallopOption[Boolean] =
      opt(name = "dry", default = Some(false), descr = "Perform a dry-run", required = false, noshort = true)
    val all: ScallopOption[Boolean] =
      opt(
        name = "all",
        default = Some(false),
        descr = "Unlike other commands, not providing any schema name will not do anything. Unless you use this option.",
        noshort = true
      )
    val items: ScallopOption[List[String]] =
      trailArg[List[String]](default = Some(List()), required = false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      if(all() && items().nonEmpty) {
        throw new FlamyException("Using the --all option will drop all schemas, and no schema name should be specified.")
      }
      val context = new FlamyContext(globalOptions, environment.get)
      context.dryRun = dryRun()
      val itemFilter = ItemFilter(items(), acceptIfEmpty = all())
      val fetcher = HiveTableFetcher(context)
      val schemaNames: Iterable[SchemaName] = fetcher.listSchemaNames.filter{itemFilter}.filterNot{_.fullName == "default"}

      val flamyRunner: FlamyRunner = FlamyRunner(context)
      val actionRunner = new ActionRunner(silentOnSuccess = false, silentOnFailure = false)
      val dropActions = schemaNames.map{schemaName => new DropSchemaAction(schemaName, flamyRunner)}
      actionRunner.run(dropActions)

      ReturnStatus(success = actionRunner.getStats.getFailCount == 0)
    }
  }

  val tables: Subcommand = new Subcommand("tables") with FlamySubcommand {
    banner("Drop the specified tables on the specified environment")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to run on", required = true, noshort = true)
    val dryRun: ScallopOption[Boolean] =
      opt(name = "dry", default = Some(false), descr = "Perform a dry-run", required = false, noshort = true)
    val all: ScallopOption[Boolean] =
      opt(
        name = "all",
        default = Some(false),
        descr = "Unlike other commands, not providing any table name will not do anything. Unless you use this option.",
        noshort = true
      )
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default = Some(List()), required = false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      if(all() && items().nonEmpty) {
        throw new FlamyException("Using the --all option will drop all tables, and no table name should be specified.")
      }
      if(!all() && items().isEmpty) {
        throw new FlamyException("If you really want to drop all the tables, you should add the --all option.")
      }
      val context = new FlamyContext(globalOptions, environment.get)
      context.dryRun = dryRun()
      val itemFilter = ItemFilter(items(), acceptIfEmpty = all())
      val fetcher = HiveTableFetcher(context)
      val tables: Iterable[TableInfo] = fetcher.listTables(itemFilter)

      val flamyRunner: FlamyRunner = FlamyRunner(context)
      val actionRunner = new ActionRunner(silentOnSuccess = false, silentOnFailure = false)
      val dropActions = tables.map{table => new DropTableAction(table, flamyRunner)}
      actionRunner.run(dropActions)

      ReturnStatus(success = actionRunner.getStats.getFailCount == 0)
    }
  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    subCommands match {
      case (command: FlamySubcommand) :: Nil =>
        command.doCommand(globalOptions, Nil)
      case _ => printHelp()
    }
    ReturnSuccess
  }
}
