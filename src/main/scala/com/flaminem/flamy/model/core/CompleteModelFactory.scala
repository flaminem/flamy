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

import com.flaminem.flamy.conf.{FlamyContext, FlamyGlobalContext}
import com.flaminem.flamy.exec.files.FileRunner
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.collection.immutable.{MergeableTableInfoCollection, TableCollection}
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.files.{FileIndex, FileType, TableFile}
import com.flaminem.flamy.model.names.TableName
import com.flaminem.flamy.model.partitions.Partition
import com.flaminem.flamy.model.partitions.transformation.Annotation
import com.flaminem.flamy.model.{TableType, _}
import com.flaminem.flamy.parsing.MetaParser
import com.flaminem.flamy.parsing.hive.{AnnotationParser, CreateTableParser, PopulateParser}
import com.flaminem.flamy.parsing.model.TableDependency
import com.flaminem.flamy.utils.logging.Logging

/**
 * This factory is used to create a complete Model, by fully parsing the Hive queries.
 */
/* Some methods are exposed to allow for testing in the class TableDependency$Test */
private[flamy]
class CompleteModelFactory(context: FlamyContext, tableGraph: TableGraph) extends Logging{
  private val preModel = tableGraph.model

  assert(preModel.isInstanceOf[IncompleteModel])

  private val runner: FileRunner = new FileRunner(silentOnSuccess = !FlamyOutput.shouldPrint(FlamyOutput.INFO))
  private[flamy] var tableDefinitions: TableCollection = TableCollection()
  private[flamy] var mergeableTableInfoSet = MergeableTableInfoCollection()

  private def checkName(table: Table, tableFile: TableFile): Unit = {
    if (!table.fullName.equalsIgnoreCase(tableFile.tableName.fullName)) {
      throw new FlamyException(
        "The table name found in the script (" + table.fullName + ")" +
          " does not match the table name in the file path (" + tableFile.tableName + ")"
      )
    }
  }

  private[flamy] def analyzeCreate(tableFile: TableFile): Unit = {
    val table: Table = CreateTableParser.parseText(tableFile.text)(context)
    checkName(table, tableFile)
    tableDefinitions += table
  }

  private def handleError(tableFile: TableFile, e: FlamyException): Unit = {
    val errorMessage =
      tableFile.fileType match {
        case FileType.VIEW => s"Error found in view ${tableFile.tableFileName}"
        case _ => s"Error found in table ${tableFile.tableFileName}"
      }
    throw new FlamyException(s"$errorMessage `${tableFile.tableName}` :\n  ${e.getMessage}" , e)
  }

  /**
    * Retrieve partition variables information from the parent tables.
    * @param tableFile
    * @return
    */
  private def getVariables(tableFile: TableFile): Variables = {
    val parentPartitions: Iterable[PartitionKey] =
      for {
        tableInfo: TableInfo <- preModel.getTable(tableFile.tableName).view
        populateInfo: PopulateInfo <- tableInfo.populateInfos.find{_.tableFile == tableFile}.view
        parent: TableName <- tableGraph.getParentsThroughViews(populateInfo).view
        parentInfo: TableInfo <- preModel.getTable(parent).view
        partition: PartitionKey <- parentInfo.partitions
      } yield {
        partition
      }
    val tablePartitions: Seq[PartitionKey] = tableDefinitions.get(tableFile.tableName).map{_.partitionKeys}.getOrElse(Nil)
//     FIXME: user tablePartitions instead of parentPartitions
    if(FlamyGlobalContext.USE_OLD_REGEN.getProperty) {
      context.getVariables.subsetInText(tableFile.text, parentPartitions)
    }
    else {
      context.getVariables.subsetInText(tableFile.text, tablePartitions)
    }
  }

  private[flamy] def analyzePopulateDependencies(tableFile: TableFile, isView: Boolean, vars: Variables = new Variables()): Seq[TableDependency] = {
    val tables: Seq[TableDependency] = PopulateParser.parseText(tableFile.text, vars, isView)(context)
    for(td <- tables) {
      checkName(td, tableFile)
      td.resolveDependencies(tableDefinitions)
      for(tableDef <- tableDefinitions.get(td.getName)) {
        td.checkColumnAndPartitions(tableDef)
        mergeableTableInfoSet += TableInfo(tableDef)
      }
    }
    tables
  }

  private[flamy] def analyzeView(tableFile: TableFile): Unit = {
    try {
      val tables: Seq[TableDependency] = PopulateParser.parseText(tableFile.text, vars = new Variables(), isView = true)(context)
      tables.foreach {
        td =>
          checkName(td, tableFile)
          td.resolveDependencies(tableDefinitions)
          mergeableTableInfoSet += ViewInfo(td, tableFile)
      }
      tableDefinitions ++= tables
    }
    catch {
      case e: FlamyException => handleError(tableFile, e)
    }
  }

  private def analyzePopulate(tableFile: TableFile) : Unit = {
    analyzePopulate(tableFile, getVariables(tableFile))
  }

  /** this method is visible for testing purposes */
  private[flamy] def analyzePopulate(tableFile: TableFile, variables: Variables): Unit = {
    try {
      val partitionTransformations: Seq[Annotation] =
        AnnotationParser.parseText(tableFile.text, new Variables(), isView = false)(context)
      val tableDeps = analyzePopulateDependencies(tableFile, isView = false, variables)
      /* For multi-insert, we get several tableDeps, and we want to create one PopulateInfo with multiple partitionConstraints */
      val partitionConstraints: Seq[Partition] = tableDeps.map{ td => new Partition(td.partitions) }
      /* Even when we don't find any tableDeps (e.g. with MSCK REPAIR TABLE), we still want to remember the populate */
      val table: TableDependency = tableDeps.reduceOption{ _ merge _ }.getOrElse{new TableDependency(TableType.REF, tableFile.tableName)}
      val populateInfo: PopulateInfo = PopulateInfo(table, tableFile, variables, partitionConstraints, partitionTransformations)
      val parents: Seq[Table] = tableGraph.getParentsThroughViews(populateInfo).flatMap{tableDefinitions.get}
      partitionTransformations.foreach{_.check(parents)}
      mergeableTableInfoSet += TableInfo(table, populateInfo)
    }
    catch {
      case e: FlamyException => handleError(tableFile, e)
    }
  }

  private def analyzeMeta(tableFile: TableFile): Unit = {
    val td: TableDependency = MetaParser.parseTableDependencies(tableFile.path.toString)
    mergeableTableInfoSet += TableInfo(td)
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

  def generateModel(): CompleteModel = {
    val fileIndex = tableGraph.model.fileIndex
    mergeableTableInfoSet = MergeableTableInfoCollection()

    runner.run(analyzeCreate(_: TableFile), fileIndex.getAllTableFilesOfType(FileType.CREATE))
    mergeableTableInfoSet ++= tableDefinitions.map{TableInfo(_)}

    runner.run(analyzeView(_: TableFile), tableGraph.getTopologicallySortedViewFiles)
    runner.run(analyzePopulate(_: TableFile), fileIndex.getAllTableFilesOfType(FileType.POPULATE))
    runner.run(analyzeMeta(_: TableFile), fileIndex.getAllTableFilesOfType(FileType.META))

    val stats: FileRunner#Stats = runner.getStats
    FlamyOutput.out.info(stats.format("analyzed"))

    if (stats.getFailCount > 0) {
      throw new FlamyException("Interrupting command, some file were not validated.")
    }

    mergeableTableInfoSet ++= getMissingTables(fileIndex)

    new CompleteModel(mergeableTableInfoSet.toTableInfoCollection, fileIndex)
  }

  def getStats: FileRunner#Stats = runner.getStats

}
