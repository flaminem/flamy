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
import com.flaminem.flamy.exec.hive.{HivePartitionFetcher, HiveTableFetcher}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.exec.utils.{ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.model.{ItemFilter, TableInfo}
import com.flaminem.flamy.model.metadata.{SchemaWithInfo, TableWithInfo}
import com.flaminem.flamy.model.names.ItemName
import com.flaminem.flamy.model.partitions.TablePartitioningInfo
import com.flaminem.flamy.utils.AutoClose
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.prettyprint.Tabulator
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Describe extends Subcommand("describe") with FlamySubcommand with Logging {

  val schemas = new Subcommand("schemas") with FlamySubcommand {
    banner("List all schemas with their properties (schema, num_tables, size, num_files, modification_time)")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=true, noshort=true)
    val machineReadable: ScallopOption[Boolean] =
      opt[Boolean](name="bytes", short = 'b', descr="Display file sizes as bytes", default = Some(false))

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      val fetcher = HiveTableFetcher(context)
      val schemas: Iterable[SchemaWithInfo] = fetcher.listSchemasWithInfo.toSeq.sortBy{_.name}
      val header: Seq[String] = SchemaWithInfo.getInfoHeader
      val table: Traversable[Seq[Any]] = schemas.map{ _.getFormattedInfo(context, humanReadable = !machineReadable()) }
      FlamyOutput.out.println(Tabulator.format(Seq(header) ++ table, leftJustify = true))
      ReturnSuccess
    }
  }

  val tables = new Subcommand("tables") with FlamySubcommand {
    banner("List all tables inside one or several schemas with their properties (table, num_partitions, size, num_files, modification_time, format)")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=true, noshort=true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()),required=false)
    val machineReadable: ScallopOption[Boolean] =
      opt[Boolean](name="bytes", short = 'b', descr="Display file sizes as bytes", default = Some(false))

    /**
      * List of information to display:
      * - table : full name of the table
      * - num_partitions: number of the partitions in the table if the table if partitionned, empty otherwise
      * - size: total size of the files (without replication)
      * - num_files: total number of files
      * - modification_time: last time the table was updated according to the Metastore (may be inaccurrate if someone tempered with the file system)
      * - format: the table format
      */
    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      val itemFilter = new ItemFilter(items(), true)
      val fetcher = HiveTableFetcher(context)
      val tables: Seq[TableWithInfo] = fetcher.listTablesWithInfo(itemFilter).toSeq.sortBy{_.name}
      val header: Seq[String] = TableWithInfo.getInfoHeader
      val table: Seq[Seq[Any]] = tables.map { _.getFormattedInfo(context, humanReadable = !machineReadable()) }
      FlamyOutput.out.println(Tabulator.format(Seq(header) ++ table, leftJustify = true))
      ReturnSuccess
    }
  }

  val partitions = new Subcommand("partitions") with FlamySubcommand {
    banner("List all partitions inside one or several schemas and/or tables with their properties (<partition_keys*>, size, num_files, modification_time, format)")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=true, short='o')
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()), required=false)
    val machineReadable: ScallopOption[Boolean] =
      opt[Boolean](name="bytes", short = 'b', descr="Display file sizes as bytes", default = Some(false))

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      for {
        fetcher: HivePartitionFetcher <- AutoClose(HivePartitionFetcher(context))
      } {
        val items: Seq[ItemName] = this.items()
        for {
          table: TableInfo <- fetcher.listTables(items: _*)
        } {
          if(table.isView){
            println(s"${table.tableName} is a view")
          }
          else{
            println(table.tableName)
            val nonRefreshedInfo = fetcher.getTablePartitioningInfo(table.tableName)
            logger.debug(s"refreshing file statuses for table ${table.tableName}")
            val tablePartitioningInfo: TablePartitioningInfo = nonRefreshedInfo.refreshAllFileStatus(context)
            logger.debug(s"file statuses refreshed for table ${table.tableName}")
            println(tablePartitioningInfo.toFormattedString(context, humanReadable = !machineReadable()))
          }
          println()
        }
      }
      ReturnSuccess
    }
  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    subCommands match {
      case (command: FlamySubcommand)::Nil =>
        command.doCommand(globalOptions, Nil)
      case _ => printHelp()
    }
    ReturnSuccess
  }

}
