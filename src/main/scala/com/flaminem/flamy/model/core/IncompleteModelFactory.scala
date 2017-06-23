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

package com.flaminem.flamy.model.core

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.exec.files.FileRunner
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.collection.immutable.MergeableTableInfoCollection
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.files.{FileIndex, FileType, TableFile}
import com.flaminem.flamy.parsing.MetaParser
import com.flaminem.flamy.parsing.hive.{CreateTableParser, PopulatePreParser}
import com.flaminem.flamy.parsing.model.{MergeableTableDependencyCollection, TableDependency}
import com.flaminem.flamy.utils.logging.Logging


/**
 * This factory creates a Flamy using the PreParsers.
 * This allow to quickly get the dependencies between tables without replacing the Variables.
 * Once the dependency graph is build, we can use perform a further analysis on the tables that are specified as arguments
 * and ignore the other one.
 */
private[core]
//TODO: there is a lot of code duplication between the IncompleteModelFactory and the CompleteModelFactory, perhaps they should inherit from the same trait?
class IncompleteModelFactory(private val context: FlamyContext) extends Logging {
  private val runner: FileRunner = new FileRunner(silentOnSuccess = true)
  private[model] var mergeableTableInfoSet = MergeableTableInfoCollection()

  private def checkName(table: Table, tableFile: TableFile) {
    if (!table.fullName.equalsIgnoreCase(tableFile.tableName.fullName)) {
      throw new FlamyException(
        "The table name found in the script (" + table.fullName + ")" +
          " does not match the table name in the file path (" + tableFile.tableName + ")"
      )
    }
  }

  private def analyzeCreate(tableFile: TableFile) {
    val table: Table = CreateTableParser.parseText(tableFile.text)(context)
    checkName(table, tableFile)
    mergeableTableInfoSet += TableInfo(table)
  }

  private def analyzePopulate(tableFile: TableFile) {
    val tables: MergeableTableDependencyCollection = PopulatePreParser.parseText(tableFile.text)
    mergeableTableInfoSet ++= tables.map{TableInfo(_, tableFile)}
    tables.foreach{table => checkName(table, tableFile)}
  }

  private def analyzeMeta(tableFile: TableFile) {
    val table: TableDependency = MetaParser.parseTableDependencies(tableFile.path.toString)
    mergeableTableInfoSet += TableInfo(table, tableFile)
//    mergeableTableDependencySet.add(table)
  }

  /**
   * Some tables may be present in the fileIndex without being in the tableDependencies,
   * for instance when a table only have a TEST.hql file. We must add them in the tableDependencies as singletons
   */
  private def getMissingTables(fileIndex: FileIndex): Iterable[TableInfo] = {
    for {
      tableName <- fileIndex.getTableNames
      if !mergeableTableInfoSet.contains(tableName)
    } yield {
      new TableInfo(TableType.REF, tableName)
    }
  }

  def generateModel(fileIndex: FileIndex): IncompleteModel = {
    logger.info("Generating model")
    mergeableTableInfoSet = MergeableTableInfoCollection()
    val creates = fileIndex.getAllTableFilesOfType(FileType.CREATE)
    val views = fileIndex.getAllTableFilesOfType(FileType.VIEW)
    val populates = fileIndex.getAllTableFilesOfType(FileType.POPULATE)
    val metas = fileIndex.getAllTableFilesOfType(FileType.META)

    runner.run(analyzeCreate(_: TableFile), creates)
    runner.run(analyzePopulate(_: TableFile), populates ++ views)
    runner.run(analyzeMeta(_: TableFile), metas)

    val stats: FileRunner#Stats = runner.getStats
    FlamyOutput.out.info(stats.format("analyzed"))

    if (stats.getFailCount > 0) {
      throw new FlamyException("Interrupting command, some file were not validated.")
    }

    mergeableTableInfoSet ++= getMissingTables(fileIndex)

    val result = new IncompleteModel(mergeableTableInfoSet.toTableInfoCollection, fileIndex)
    logger.info("model generated")
    result
  }

  def getStats: FileRunner#Stats = runner.getStats

}
