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
import com.flaminem.flamy.exec.files.FileRunner
import com.flaminem.flamy.exec.hive.{HiveTableFetcher, ModelHiveTableFetcher}
import com.flaminem.flamy.exec.utils._
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.collection.immutable.TableInfoCollection
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.files.FileType
import com.flaminem.flamy.model.names.{ItemName, SchemaName, TableName}
import com.flaminem.flamy.utils.{AutoClose, DiffUtils}
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import scala.language.reflectiveCalls

/**
  * Created by fpin on 5/22/15.
  */
class Push extends Subcommand("push") with FlamySubcommand {

  val schemas = new Subcommand("schemas") with FlamySubcommand {
    banner("Create on the specified environment the schemas that are present in the model and missing in the environment")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to run on", required = true, noshort = true)
    val dryRun: ScallopOption[Boolean] =
      opt(name = "dry", default = Some(false), descr = "Perform a dry-run", required = false, noshort = true)
    val items: ScallopOption[List[String]] =
      trailArg[List[String]](default = Some(List()), required = false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val sourceContext = new FlamyContext(globalOptions)
      val destContext = new FlamyContext(globalOptions, environment.get)
      sourceContext.dryRun = dryRun()
      destContext.dryRun = dryRun()

      val itemFilter = new ItemFilter(items(), true)
      val sourceFetcher = new ModelHiveTableFetcher(sourceContext, items(): _*)
      val destFetcher = HiveTableFetcher(destContext)
      val sourceSchemas: Set[SchemaName] = sourceFetcher.listSchemaNames.filter{itemFilter}.toSet
      val destSchemas: Set[SchemaName] = destFetcher.listSchemaNames.filter{itemFilter}.toSet

      val fileRunner: FileRunner = new FileRunner(silentOnSuccess = false, silentOnFailure = false)
      val flamyRunner: FlamyRunner = FlamyRunner(destContext)
      val fileIndex = sourceFetcher.fileIndex.filter{ItemFilter(sourceSchemas.diff(destSchemas), acceptIfEmpty = false)}

      fileRunner.run(flamyRunner.runCreateSchema(_), fileIndex.getAllSchemaFilesOfType(FileType.CREATE_SCHEMA))
      ReturnStatus(success = fileRunner.getStats.getFailCount == 0)
    }

  }

  val tables = new Subcommand("tables") with FlamySubcommand {
    banner("Create on the specified environment the tables that are present in the model and missing in the environment")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to run on", required = false, noshort = true)
    val dryRun: ScallopOption[Boolean] =
      opt(name = "dry", default = Some(false), descr = "Perform a dry-run", required = false, noshort = true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default = Some(List()), required = false)

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val sourceContext = new FlamyContext(globalOptions)
      val destContext = new FlamyContext(globalOptions, environment.get)
      sourceContext.dryRun = dryRun()
      destContext.dryRun = dryRun()

      val itemFilter = new ItemFilter(items(), true)
      val sourceFetcher = HiveTableFetcher(sourceContext)
      val destFetcher = HiveTableFetcher(destContext)
      val sourceTables: Set[TableName] = sourceFetcher.listTableNames.filter{itemFilter}.toSet
      val destTables: Set[TableName] = destFetcher.listTableNames.filter{itemFilter}.toSet

      val fileRunner: FileRunner = new FileRunner(silentOnSuccess = false, silentOnFailure = false)
      val flamyRunner: FlamyRunner = FlamyRunner(destContext)

      val itemsToPush: Seq[TableName] = sourceTables.diff(destTables).toSeq

      if (itemsToPush.nonEmpty) {
        val graph = TableGraph(sourceContext, itemsToPush, checkNoMissingTable = true).subGraph(itemsToPush)
        fileRunner.run(flamyRunner.runCreateTable(_), graph.model.fileIndex.getAllTableFilesOfType(FileType.CREATE))
        fileRunner.run(flamyRunner.runCreateTable(_), graph.getTopologicallySortedViewFiles)
      }
      ReturnStatus(success = fileRunner.getStats.getFailCount == 0)
    }
  }

  val columns = new Subcommand("columns") with FlamySubcommand {
    banner("Alter the columns of the table on the specified environment to match the table in the local model. Currently only supports column renaming.")
    val environment: ScallopOption[Environment] =
      opt(name = "on", descr = "Specifies environment to run on", required = false, noshort = true)
    val dryRun: ScallopOption[Boolean] =
      opt(name = "dry", default = Some(false), descr = "Perform a dry-run", required = false, noshort = true)
    val items: ScallopOption[List[ItemName]] =
      trailArg[List[ItemName]](default = Some(List()), required = false)

    def pushCols(sourceFetcher: HiveTableFetcher, destFetcher: HiveTableFetcher, flamyRunner: FlamyRunner): ReturnStatus = {
      val itemFilter = new ItemFilter(items(), true)
      val sourceTables: TableInfoCollection = TableInfoCollection(sourceFetcher.listTables{itemFilter}.toSeq:_*)
      val destTables: TableInfoCollection = TableInfoCollection(destFetcher.listTables{itemFilter}.toSeq:_*)
      val tablesToPush: Seq[String] = sourceTables.toSet.intersect(destTables.toSet).toSeq.map{_.fullName}

      val runner = new ActionRunner()
      def renameTableColsAction(table: TableName): Option[Action] = {
        val sourceTable: TableInfo = sourceTables.get(table).get
        val destTable: TableInfo = destTables.get(table).get
        val statements: Seq[String] =
          DiffUtils.hammingDiff(sourceTable.columns.toIndexedSeq, destTable.columns.toIndexedSeq, allowReplacements = true).flatMap {
            case (s, d) if s == d => None
            case (Some(s), Some(d)) => Some(s, d)
            case _ => throw new FlamyException("")
          }
          .map {
            case (s, d) => s"ALTER TABLE ${table.fullName} CHANGE COLUMN ${d.columnName} ${s.columnName} ${s.columnType.get}"
          }
        if(statements.isEmpty){
          None
        }
        else {
          Some(
            new Action {
              override val name: String = s"ALTER TABLE ${table.fullName}"
              override val logPath: String = s"ALTER_TABLE_${table.fullName}"
              override def run(): Unit = {
                if(dryRun()){
                  statements.foreach{ThreadPrintStream.systemOut.println}
                }
                else {
                  statements.foreach{flamyRunner.runText(_)}
                }
              }
            }
          )
        }
      }
      val actions: Seq[Action] = tablesToPush.flatMap{renameTableColsAction(_)}
      runner.run(actions)
      ReturnStatus(success = runner.getStats.getFailCount == 0)
    }

    override def doCommand(globalOptions: FlamyGlobalOptions, subCommands: List[ScallopConf]): ReturnStatus = {
      val sourceContext = new FlamyContext(globalOptions)
      val destContext = new FlamyContext(globalOptions, environment.get)
      sourceContext.dryRun = dryRun()
      destContext.dryRun = dryRun()
      for{
        sourceFetcher: HiveTableFetcher <- AutoClose(HiveTableFetcher(sourceContext))
        destFetcher: HiveTableFetcher <- AutoClose(HiveTableFetcher(destContext))
        flamyRunner: FlamyRunner <- AutoClose(FlamyRunner(destContext))
      } yield {
        pushCols(sourceFetcher, destFetcher, flamyRunner)
      }
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
