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

package com.flaminem.flamy.exec.hive

import java.io.File
import java.sql.SQLException

import com.flaminem.flamy.conf.{Environment, FlamyConfVars, FlamyContext}
import com.flaminem.flamy.exec.files.{FileRunner, ItemFileAction}
import com.flaminem.flamy.exec.utils.Action
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model._
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.exceptions.FailedQueryException
import com.flaminem.flamy.model.files._
import com.flaminem.flamy.parsing.hive.QueryUtils
import com.flaminem.flamy.utils.sql._
import com.flaminem.flamy.utils.sql.hive.StreamedResultSet

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/**
  * A HiveRunner provides all the necessary methods to execute queries on a Hive environment.
  *
  * Implementing classes should have a close() method to allow systematic closing of connections.
  * It should provide the following guarantees:
  * - If close() is called and no other method is called afterwards then we guarantee that no connection or other closeable resource is left open.
  * - If another method is called after close() has been called, this method should work normally (by re-opening the previously closed resources if necessary).
  *
  * This means that a HiveRunner must still be functional after being closed.
  *
  * @param context
  */
//noinspection ScalaStyle
abstract class HiveRunner(val context: FlamyContext) extends AutoCloseable {

  /**
    * Run a query and ignore the result.
    *
    * @param query
    * @param title title of the query. May be use to display information
    * @return
    */
  protected def runPreparedQuery(query: String, title: Option[String]): Int

  /**
    * Run a query and optionally returns the result.
    *
    * @param query
    * @param title title of the query. May be use to display information
    * @return
    */
  protected def executePreparedQuery(query: String, title: Option[String]): StreamedResultSet

  def runCreate(text: String, variables: Variables): Unit

  def runCheck(text: String, variables: Variables): Unit

  val dryRun: Boolean = context.dryRun

  private def addPrefix(query: String, prefix: String) = {
    val lowerCaseQuery: String = query.toUpperCase
    if (lowerCaseQuery.startsWith("SET") || lowerCaseQuery.startsWith("USE") || lowerCaseQuery.startsWith("MSCK") || lowerCaseQuery.startsWith("ADD JAR")) {
      query
    }
    else {
      prefix + query
    }
  }

  private def addExplainPrefix(query: String): String = {
    addPrefix(query, "EXPLAIN ")
  }

  private def addExplainDependencyPrefix(query: String): String = {
    addPrefix(query, "EXPLAIN DEPENDENCY ")
  }

  protected def handleQueryFailure(query: String, e: Throwable): Nothing = {
    System.err.println(f"FAILED query : \n$query")
    Option(e.getMessage).foreach{msg => System.err.println(f"MESSAGE : \nf$msg")}
    System.err.flush()
    throw new FailedQueryException(e.getMessage, e)
  }


  private def prepareQuery(query: String, variables: Variables, explainIfDryRun: Boolean): String = {
    try {
      val buildQ: String = variables.replaceInText(query)
      if (dryRun && explainIfDryRun) {
        addExplainPrefix(buildQ)
      }
      else{
        buildQ
      }
    }
    catch {
      case NonFatal(e) => handleQueryFailure(query, e)
    }
  }

  def runQuery(query: String, title: Option[String], variables: Variables, explainIfDryRun: Boolean): Int = {
    val preparedQuery: String = prepareQuery(query, variables, explainIfDryRun)
    Try {
      FlamyOutput.out.info("Running query:\n" + preparedQuery)
      runPreparedQuery(preparedQuery, title)
    }
    match {
      case Failure(e) => handleQueryFailure(preparedQuery, e)
      case Success(returnValue) if returnValue > 0 => throw new FailedQueryException
      case Success(returnValue) => returnValue
    }
  }

  /**
    * Should only be used to execute small queries, for experimental features
    * @param query
    */
  def executeQuery(query: String): StreamedResultSet = {
    executeQuery(query, None, new Variables, explainIfDryRun = true)
  }

  def getInputsFromQuery(query: String, variables: Variables): Seq[Inputs] = {
    Try {
      val preparedQuery = addExplainDependencyPrefix(prepareQuery(query, variables, explainIfDryRun = false))
      executePreparedQuery(preparedQuery, None)
    }
    match {
      case Failure(e: java.sql.SQLException) =>
        /* For EXPLAIN DEPENDENCY, we ignore java.sql.SQLExceptions, as the Spark ThrifServer does not support this command yet */
        Nil
      case Failure(e) =>
        handleQueryFailure(query, e)
        throw new FailedQueryException(e.getMessage, e)
      case Success(returnValue) =>
        returnValue.map{row => Inputs.fromJson(row.head)}.toSeq
    }
  }

  def executeQuery(query: String, title: Option[String], variables: Variables, explainIfDryRun: Boolean): StreamedResultSet = {
    Try{
      val preparedQuery = prepareQuery(query, variables, explainIfDryRun)
      executePreparedQuery(preparedQuery, title)
    }
    match {
      case Failure(e) => handleQueryFailure(query, e)
        throw new FailedQueryException(e.getMessage, e)
      case Success(returnValue) => returnValue
    }
  }

  /**
    * Uses Hive's EXPLAIN DEPENDENCY to obtain the list of input tables and partitions.
    * @param populateInfo
    * @param closeAfter
    */
  def getInputsFromPopulate(populateInfo: PopulateInfo, closeAfter: Boolean = true): Seq[Inputs] = {
    getInputsFromText(populateInfo.tableFile.text, populateInfo.variables, closeAfter)
  }

  def runPopulate(populateInfo: PopulateInfo, closeAfter: Boolean = true): Unit = {
    runText(populateInfo.tableFile.text, Some(s"POPULATE ${populateInfo.title}"), populateInfo.variables, explainIfDryRun = true, closeAfter)
  }

  def runPresets(file: PresetsFile): Unit = runText(file.text, None, context.getVariables, explainIfDryRun = false, closeAfter = false)
  def runCheckTable(file: TableFile): Unit = runCheck(file.text, context.getVariables)
  def runCheckSchema(file: SchemaFile): Unit = runCheck(file.text, context.getVariables)
  def runCheckView(file: TableFile): Unit = runCheck(file.text, context.getVariables)
  def runCreateTable(file: TableFile): Unit = runCreate(file.text, context.getVariables)
  def runCreateView(file: TableFile): Unit = runCreate(file.text, context.getVariables)
  def runCreateSchema(file: SchemaFile): Unit = runCreate(file.text, context.getVariables)
  def runTest(file: TableFile, variables: Variables): Unit = {
    runTestText(file.text, Some(s"TEST ${file.tableName.toString}"), variables, explainIfDryRun = true)
  }


  def checkResults(resultSet: StreamedResultSet): Option[String] = {
    if(!resultSet.hasNext) {
      Some("Results are empty")
    }
    else {
      val row: ResultRow = resultSet.next
      if(resultSet.hasNext) {
        Some(
          s"""Tests should output only one row : got
             |$row
             |${resultSet.next}
             |"""".stripMargin
        )
      }
      else {
        row.headOption
      }
    }
  }

  def runTestQuery(query: String, title: Option[String], variables: Variables, explainIfDryRun: Boolean): Unit = {
    val preparedQuery = prepareQuery(query, variables, explainIfDryRun)
    val resultSet = executePreparedQuery(preparedQuery, title)
    val result: Option[String] =
       if(dryRun && explainIfDryRun) {
         None
       }
       else {
         checkResults(resultSet)
       }
    result match {
      case Some(s) if s.startsWith("ok") =>
        FlamyOutput.out.success(s)
      case Some(s) =>
        FlamyOutput.out.failure(s)
      case None => ()
    }
  }

  def runTestText(text: String, title: Option[String], variables: Variables, explainIfDryRun: Boolean): Unit = {
    QueryUtils.cleanAndSplitQuery(text).foreach{ query => runTestQuery(query, title, variables, explainIfDryRun) }
  }

  /**
    * Execute a text command.
    *
    * @param text    text of comma-separated queries to execute
    * @param title   title of the query, used to define the name of the YARN job
    * @param explainIfDryRun    add explain before each query if dry-run mode is activated
    * @param closeAfter    if true, the method close() will be called after the command is executed (optional, default = true).
    */
  def runText(text: String, title: Option[String], variables: Variables, explainIfDryRun: Boolean, closeAfter: Boolean = true): Unit = {
    QueryUtils.cleanAndSplitQuery(text).foreach {
      query => runQuery(query, title, variables, explainIfDryRun)
    }
    if(closeAfter) {
      close()
    }
    else {
      runPreparedQuery("USE default", None)
    }
  }

  /**
    * Uses Hive's EXPLAIN DEPENDENCY to obtain the list of input tables and partitions.
    *
    * @param text    text of comma-separated queries to execute
    * @param closeAfter    if true, the method close() will be called after the command is executed (optional, default = true).
    */
  def getInputsFromText(text: String, variables: Variables, closeAfter: Boolean = true): Seq[Inputs] = {
    val res: Seq[Inputs] =
      QueryUtils.cleanAndSplitQuery(text).flatMap {
        query => getInputsFromQuery(query, variables)
      }
    if(closeAfter) {
      close()
    }
    else {
      runPreparedQuery("USE default", None)
    }
    res
  }

  private val fileRunner: FileRunner = new FileRunner

  def checkAll(graph: TableGraph): Unit = {
    fileRunner.run(runCheckSchema(_) , graph.model.fileIndex.getAllSchemaFilesOfType(FileType.CREATE_SCHEMA))
    fileRunner.run(runCheckTable(_) , graph.getTableFilesOfType(FileType.CREATE))
    fileRunner.run(runCheckView(_) , graph.getTopologicallySortedViewFiles)
  }

  def runPresets(context: FlamyContext): Unit = {
    context.HIVE_PRESETS_PATH.getProperty match {
      case Some(path) =>
        FlamyOutput.out.println(f"Running presets: $path")
        fileRunner.run(runPresets(_:PresetsFile), new PresetsFile(new File(path))::Nil)
      case _ =>
        FlamyOutput.out.println("No presets to run")
    }
  }

  def populateAll(model: Model, context: FlamyContext): Unit = {
    runPresets(context)
    val actions: Iterable[Action] =
      for {
        tableInfo: TableInfo <- model.getAllTables
        populateInfo: PopulateInfo <- tableInfo.populateInfos
        tableFile: TableFile = populateInfo.tableFile
      } yield {
        new ItemFileAction(tableFile) {
          override def run(): Unit = {
            /* We replace partition values with dummy constant so that the validation passes */
            val vars = populateInfo.variables.cancelPartitions(tableInfo.partitions)
            runPopulate(populateInfo.copy(variables = vars), closeAfter = false)
          }
        }
      }
    fileRunner.run(actions)
    close()
  }

  def testAll(fileIndex: FileIndex, context: FlamyContext): Unit = {
    fileRunner.run(runTest(_ : TableFile, context.getVariables) , fileIndex.getAllTableFilesOfType(FileType.TEST))
  }

  def getStats: FileRunner#Stats = {
    fileRunner.getStats
  }

  /**
    * Retrieve the value of given configuration parameter.
    *
    * @param key
    * @return
    */
  def getConfiguration(key: String): Option[String]

  /**
    * Override this to make the implementation AutoCloseable.
    * This method should provide the following guarantee :
    * - If close() is called and no other method is called afterwards then we guarantee that no connection or other closeable resource is left open.
    * - If another method is called after close() has been called,
    *   this method should work normally (by re-opening the previously closed resources if necessary).
    */
  def close(): Unit

  def interrupt(): Unit

}


object HiveRunner {

  def apply(context: FlamyContext): HiveRunner = {
    val runner: HiveRunner =
      if (context.env == Environment.MODEL_ENV) {
        new ModelHiveRunner(context)
      }
      else if (context.HIVE_RUNNER_TYPE.getProperty.toLowerCase == "local") {
        new LocalHiveRunner(context)
      }
      else {
          new RemoteHiveRunner(context)
        }
      runner.runPresets(context)
    runner
  }

}