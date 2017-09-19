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
import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyContextFormatter, FlamyGlobalOptions}
import com.flaminem.flamy.exec.hive.{HivePartitionFetcher, HiveTableFetcher}
import com.flaminem.flamy.exec.utils._
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName, TablePartitionName}
import com.flaminem.flamy.model.partitions.TablePartitioningInfo
import com.flaminem.flamy.utils.AutoClose
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.prettyprint.Tabulator
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Show extends Subcommand("show") with FlamySubcommand with Logging {

  val conf =  new Subcommand("conf") with FlamySubcommand {
    banner("Print all the values that are active in the configuration for the specified environment.")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment for which the active config should be printed", required = false, noshort = true)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      FlamyOutput.out.println(new FlamyContextFormatter(context).format())
      ReturnSuccess
    }
  }

  val schemas = new Subcommand("schemas") with FlamySubcommand {
    banner("Show schemas")

    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to run on", required = false, noshort = true)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      val fetcher = HiveTableFetcher(context)
      val schemas: Iterable[SchemaName] = fetcher.listSchemaNames
      val allTables: Iterable[TableName] = fetcher.listTableNames
      val sb: StringBuilder = new StringBuilder(f"Found ${schemas.size} schemas with ${allTables.size} tables:")
      for (schema <- schemas) {
        val tables = fetcher.listTablesNamesInSchema(schema)
        sb.append("\n    - " + schema + " (" + tables.size + ")")
      }
      println(sb)
      ReturnSuccess
    }

  }

  val tables = new Subcommand("tables") with FlamySubcommand {
    banner("Show tables in specified schemas")

    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=false, noshort=true)

    val formatted: ScallopOption[Boolean] =
      opt(name="formatted", default = Some(false), descr="Print results in a table", required=false, noshort=true)

    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()),required=false)

    private val from: ScallopOption[List[String]] =
      opt[List[String]](name="from", default=Some(Nil), descr="Start from the given schemas/tables.", noshort=true, argName = "items")

    private val to: ScallopOption[List[String]] =
      opt[List[String]](name="to", default=Some(Nil), descr="Stop at the given schemas/tables.", noshort=true, argName = "items")

    codependent(from, to)
    conflicts(items, List(from, to))
    /* options '--from' and '--to' cannot be used with '--on ENV' as we may have discrepancies between the model and the environment */
    conflicts(environment, List(from, to))

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, this.environment.get)
      val tables: Seq[TableName] =
        ItemArgs(items(), from(), to()) match {
          case ItemList(l) =>
            val itemFilter = ItemFilter(l, acceptIfEmpty = true)
            val fetcher = HiveTableFetcher(context)
            fetcher.listTableNames(itemFilter).toSeq
          case itemArgs : ItemRange =>
            val model = Model.getIncompleteModel(context, Nil)
            val g = TableGraph(model).subGraph(itemArgs)
            g.vertices
        }

      val schemas: Seq[(SchemaName, Seq[TableName])] = tables.groupBy{_.schemaName}.toSeq.sortBy{_._1}

      val header = s"Found ${tables.size} tables in ${schemas.size} schemas"
      if(formatted()){
        val res = Tabulator.format(Seq(header) +: tables.sorted.map{Seq(_)}, leftJustify = true)
        FlamyOutput.out.println(res)
      }
      else {
        val sb = new StringBuilder(s"$header :")
        for ((schema, tables) <- schemas) {
          sb.append("\n    - " + schema + " (" + tables.size + ") : ")
          for (table <- tables.sorted) {
            sb.append("\n        - " + table.name)
          }
        }
        FlamyOutput.out.println(sb.toString())
      }
      ReturnSuccess
    }
  }

  val partitions = new Subcommand("partitions") with FlamySubcommand {
    banner("Show partitions in schemas and tables")

    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=true, short='o')

    val formatted: ScallopOption[Boolean] =
      opt(name="formatted", default = Some(false), descr="Print results in a table", required=false, noshort=true)

    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()), required=false)

    val machineReadable: ScallopOption[Boolean] =
      opt[Boolean](name="bytes", short = 'b', descr="Display file sizes as bytes", default = Some(false))

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      for {
        fetcher: HivePartitionFetcher <- AutoClose(HivePartitionFetcher(context))
      } {
        val partitions: Seq[TablePartitionName] =
          fetcher.listTables(items(): _*).flatMap{
            table => fetcher.listPartitionNames(table.tableName)
          }.toSeq
        val tables: Seq[(TableName, Seq[TablePartitionName])] =
          partitions.groupBy{_.tableName}.toSeq.sortBy{_._1}

        val header = s"Found ${partitions.size} partitions in ${tables.size} tables"
        if(formatted()){
          val res = Tabulator.format(Seq(header) +: partitions.sorted.map{Seq(_)}, leftJustify = true)
          println(res)
        }
        else {
          val sb = new StringBuilder(s"$header :")
          for ((table, partitions) <- tables) {
            sb.append("\n    - " + table + " (" + partitions.size + ") : ")
            for (partition <- partitions.sorted) {
              sb.append("\n        - " + partition.partitionName)
            }
          }
          println(sb)
        }
      }
      ReturnSuccess
    }
  }

  val graph = new ShowGraph

  val select: Subcommand = new Subcommand("select") with FlamySubcommand {
    banner("Generate a select statement for the specified table")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to run on", required = false, noshort = true)

    val tables: ScallopOption[List[TableName]] =
      trailArg[List[TableName]](default = Some(List()), required = true)

    private def quote(s: String) = {
      if (s.contains('.')) {
        "`" + s + "`"
      }
      else {
        s
      }
    }

    private def generateSelect(table: TableInfo): String = {
      s"""SELECT
          |  ${table.columnAndPartitionNames.map{quote}.mkString(",\n  ")}
          |FROM ${table.fullName}
          |LIMIT 100
          |;
          |""".stripMargin
    }

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val context = new FlamyContext(globalOptions, environment.get)
      val fetcher = HiveTableFetcher(context)
      for {
        tableName: TableName <- tables()
      } {
        fetcher.getTable(tableName) match {
          case Some(table) =>
            val select = generateSelect(table)
            FlamyOutput.out.println(select)
          case None =>
            FlamyOutput.out.error(s"Table not found: $tableName")
        }
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
