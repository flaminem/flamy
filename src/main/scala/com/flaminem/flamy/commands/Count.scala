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
import com.flaminem.flamy.exec.hive.{HivePartitionFetcher, HiveTableFetcher, RemoteHiveRunner}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.exec.utils.{ReturnFailure, ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.model.ItemFilter
import com.flaminem.flamy.model.names.{ItemName, TableName, TablePartitionName}
import com.flaminem.flamy.utils.AutoClose
import com.flaminem.flamy.utils.prettyprint.Tabulator
import com.flaminem.flamy.utils.sql.hive.StreamedResultSet
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Count extends Subcommand("count") with FlamySubcommand {

  val tables = new Subcommand("tables") with FlamySubcommand {
    banner("Execute a select count(1) on every specified table.")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=false, noshort=true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default = Some(List()), required = false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      val itemFilter = ItemFilter(items(), acceptIfEmpty = true)
      val fetcher = HiveTableFetcher(context)
      val tables: Iterable[TableName] = fetcher.listTableNames.filter{itemFilter}

      val hiveRunner: RemoteHiveRunner = new RemoteHiveRunner(context)
      try {
        for {
          tableName <- tables if !Thread.currentThread().isInterrupted
        } try {
          val res: StreamedResultSet = hiveRunner.executeQuery(f"SELECT COUNT(1) FROM $tableName")
          val row = res.next()
          FlamyOutput.out.success(f"ok: $tableName : ${row(0)}")
        } catch {
          case e: Throwable =>
            e.printStackTrace()
            FlamyOutput.err.failure(f"not ok: $tableName : ${e.getMessage}")
        }
      }
      finally{
        hiveRunner.close()
      }
      ReturnSuccess
    }

  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    subCommands match {
      case  (command: FlamySubcommand)::Nil => command.doCommand(globalOptions, Nil)
      case Nil => throw new IllegalArgumentException("A subcommand is expected")
      case _ =>
        printHelp()
        ReturnFailure
    }
  }

}
