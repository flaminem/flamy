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
import com.flaminem.flamy.exec.hive.HiveTableFetcher
import com.flaminem.flamy.exec.utils.{Action, ActionRunner, ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.model.ItemFilter
import com.flaminem.flamy.model.names.{ItemName, TableName}
import com.flaminem.flamy.utils.AutoClose
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Repair extends Subcommand("repair") with FlamySubcommand{

  val tables = new Subcommand("tables") {
    banner("Execute a msck repair table on every specified table. " +
      "This will automatically add to the metastore the partitions that exists on hdfs but not yet in the metastore.")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required = true, noshort=true)
    val dryRun: ScallopOption[Boolean] =
      opt(name="dry", default=Some(false), descr="Perform a dry-run", required = false, noshort=true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()),required = false)
  }

  private class RepairTableAction(runner: FlamyRunner, tableName: TableName) extends Action{

    @throws(classOf[Exception])
    override def run(): Unit = {
      runner.runText(f"use ${tableName.schemaName} ; MSCK REPAIR TABLE ${tableName.name}")
    }

    override val name: String = tableName.fullName
    override val logPath: String = f"${tableName.schemaName}.db/${tableName.name}/REPAIR.hql"
  }

  private def repairTables(context: FlamyContext, items: ItemName*): Unit = {
    val itemFilter = new ItemFilter(items, acceptIfEmpty = true)
    val fetcher = HiveTableFetcher(context)
    val tables: Iterable[TableName] = fetcher.listTables(itemFilter).filterNot{_.isView}.filter{_.isPartitioned}.map{_.tableName}

    val actionRunner: ActionRunner = new ActionRunner()
    for {
      flamyRunner: FlamyRunner <- AutoClose(FlamyRunner(context))
    } {
      val actions = tables.map{tableName => new RepairTableAction(flamyRunner, tableName)}
      actionRunner.run(actions)
    }
  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    subCommands match {
      case (command@this.tables) :: Nil =>
        val context = new FlamyContext(globalOptions, command.environment.get)
        context.dryRun = command.dryRun()
        repairTables(context, command.items():_*)
      case _ => printHelp()
    }
    ReturnSuccess
  }


}
