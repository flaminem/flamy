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
import com.flaminem.flamy.exec.hive.HiveTableFetcher
import com.flaminem.flamy.exec.utils.{ReturnStatus, ReturnSuccess}
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import com.flaminem.flamy.utils.DiffUtils
import com.flaminem.flamy.utils.prettyprint.Tabulator
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
 * Created by fpin on 5/22/15.
 */
class Diff extends Subcommand("diff") with FlamySubcommand{

  val schemas = new Subcommand("schemas") {
    banner("Show the schemas differences between the specified environment and the modeling environment")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=true, noshort=true)
  }

  val tables = new Subcommand("tables") {
    banner("Show the table differences between the specified environment and the modeling environment")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=false, noshort=true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()),required=false)
  }

  val columns = new Subcommand("columns") {
    banner("Show the table differences between the specified environment and the modeling environment")
    val environment: ScallopOption[Environment] =
      opt(name="on", descr="Specifies environment to run on", required=false, noshort=true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default=Some(List()),required=false)
  }

  private def diffSchemas(leftContext: FlamyContext, rightContext: FlamyContext) {
    val leftFetcher = HiveTableFetcher(leftContext)
    val rightFetcher = HiveTableFetcher(rightContext)
    val leftSchemas: Set[SchemaName] = leftFetcher.listSchemaNames.toSet
    val rightSchemas: Set[SchemaName] = rightFetcher.listSchemaNames.toSet

    val diff = DiffUtils.hammingDiff(leftSchemas.toIndexedSeq.sorted, rightSchemas.toIndexedSeq.sorted, allowReplacements = false)
    val results: Seq[Seq[String]] =
      diff.flatMap{
        case (l, r) if l == r => None
        case (l, r) =>
          Some(l.map{_.toString}.getOrElse("")::r.map{_.toString}.getOrElse("")::Nil)
      }

    val header = Seq(leftContext.env,rightContext.env)
    println(Tabulator.format(header+:results,leftJustify = true))
  }

  private[commands]
  def diffTables(leftContext: FlamyContext, rightContext: FlamyContext, items: ItemName*): String = {
    val itemFilter = new ItemFilter(items,true)
    val leftFetcher = HiveTableFetcher(leftContext)
    val rightFetcher = HiveTableFetcher(rightContext)
    val leftTables: Iterable[TableName] = leftFetcher.listTableNames.filter{itemFilter}
    val rightTables: Iterable[TableName] = rightFetcher.listTableNames.filter{itemFilter}
    val diff = DiffUtils.hammingDiff(leftTables.toIndexedSeq.sorted, rightTables.toIndexedSeq.sorted, allowReplacements = false)
    val results: Seq[Seq[String]] =
      diff.flatMap{
        case (l, r) if l == r => None
        case (l, r) => Some(l.map{_.toString}.getOrElse("")::r.map{_.toString}.getOrElse("")::Nil)
      }

    val header = Seq(leftContext.env,rightContext.env)
    Tabulator.format(header+:results,leftJustify = true)
  }

  private def formatColumn(col: Column): String = {
    val comment = col.comment.map{c => s""" COMMENT "$c" """}.getOrElse("")
    s"    ${col.columnName}${col.columnType.map{t => " " + t.toUpperCase}.getOrElse("")}$comment"
  }

  private def formatPartition(col: PartitionKey): String = {
    val comment = col.comment.map{c => s""" COMMENT "$c" """}.getOrElse("")
    s"    * ${col.columnName}${col.columnType.map{t => " " + t.toUpperCase}.getOrElse("")}$comment"
  }

  private def diffColumnsInTables(leftTable: TableInfo, rightTable: TableInfo): Seq[Seq[String]] = {
    val columnDiff: Seq[Seq[String]] =
      DiffUtils.hammingDiff(leftTable.columns.toIndexedSeq, rightTable.columns.toIndexedSeq, allowReplacements = true).flatMap {
        case (l, r) if l == r =>
          None
        case (l, r) =>
          Some(l.map{formatColumn}.getOrElse("") :: r.map{formatColumn}.getOrElse("") :: Nil)
      }

    val partitionDiff: Seq[Seq[String]] =
      DiffUtils.hammingDiff(leftTable.partitions.toIndexedSeq, rightTable.partitions.toIndexedSeq, allowReplacements = true).flatMap {
        case (l, r) if l == r =>
          None
        case (l, r) =>
          Some(l.map{formatPartition}.getOrElse("") :: r.map{formatPartition}.getOrElse("") :: Nil)
      }

    val diff = columnDiff++partitionDiff

    if (diff.isEmpty) {
      Nil
    }
    else {
      (leftTable.fullName.toString :: rightTable.fullName.toString :: Nil) +: diff
    }
  }

  private[commands]
  def diffColumns(leftContext: FlamyContext, rightContext: FlamyContext, items: ItemName*): String = {
    val itemFilter = new ItemFilter(items, true)
    val leftFetcher = HiveTableFetcher(leftContext)
    val rightFetcher = HiveTableFetcher(rightContext)
    val leftTables: Set[TableInfo] = leftFetcher.listTables{itemFilter}.toSet
    val rightTables: Set[TableInfo] = rightFetcher.listTables{itemFilter}.toSet
    val diff: IndexedSeq[(Option[TableInfo], Option[TableInfo])] =
      DiffUtils.hammingDiff(leftTables.toIndexedSeq.sorted, rightTables.toIndexedSeq.sorted, allowReplacements = false)

    val results: Seq[Seq[String]] =
      diff.flatMap{
        case (Some(leftTable), Some(rightTable)) if !leftTable.isView || !rightTable.isView =>
          diffColumnsInTables(leftTable, rightTable)
        case (l, r) if l == r => None
        case (l, r) =>
          Some(l.map{_.fullName.toString}.getOrElse("")::r.map{_.fullName.toString}.getOrElse("")::Nil)
      }
    val header = Seq(leftContext.env,rightContext.env)
    Tabulator.format(header+:results,leftJustify = true)
  }

  override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
    subCommands match {
      case (command@this.schemas) :: Nil =>
        val leftContext = new FlamyContext(globalOptions)
        val rightContext = new FlamyContext(globalOptions, command.environment.get)
        diffSchemas(leftContext, rightContext)
      case (command@this.tables) :: Nil =>
        val leftContext = new FlamyContext(globalOptions)
        val rightContext = new FlamyContext(globalOptions, command.environment.get)
        println(diffTables(leftContext, rightContext, command.items():_*))
      case (command@this.columns) :: Nil =>
        val leftContext = new FlamyContext(globalOptions)
        val rightContext = new FlamyContext(globalOptions, command.environment.get)
        println(diffColumns(leftContext, rightContext, command.items():_*))
      case _ => printHelp()
    }
    ReturnSuccess
  }

}
