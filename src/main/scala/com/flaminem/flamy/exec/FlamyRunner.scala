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

package com.flaminem.flamy.exec

import com.flaminem.flamy.conf.{FlamyConfVars, FlamyContext}
import com.flaminem.flamy.exec.files.FileRunner
import com.flaminem.flamy.exec.hive.{HiveRunner, ModelHiveRunner, RemoteHiveRunner}
import com.flaminem.flamy.exec.utils.io.FlamyOutput
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.core.Model
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.files.{FileIndex, SchemaFile, TableFile}
import com.flaminem.flamy.model.{Inputs, PopulateInfo, Variables}
import com.flaminem.flamy.utils.sql.hive.StreamedResultSet

/**
 * The FlamyRunner is used to execute the queries on local or remote.
 * It is more general than the HiveRunner as it is planned to be able to run on other backends than Hive.
 */
class FlamyRunner private(private val hiveRunner: HiveRunner) extends AutoCloseable{

  /**
    * Check that all the schemas, tables and views are correct.
    * In model mode, since the database is empty at the start of each flamy command, this method will create the schema, table and views.
    * @param graph
    */
  @throws(classOf[FlamyException])
  def checkAll(graph: TableGraph): Unit = {
    hiveRunner.checkAll(graph)
    val stats = getStats
    FlamyOutput.out.info(stats.format())
    if (stats.getFailCount > 0) {
      throw new FlamyException("Interrupting command, some queries failed.")
    }
  }

  def populateAll(model: Model, context: FlamyContext): Unit = {
    val stats = getStats
    stats.clear()
    hiveRunner.populateAll(model, context)
    FlamyOutput.out.info(stats.format())
  }

  def getInputsFromText(text: String): Seq[Inputs] = {
    hiveRunner.getInputsFromText(text, variables = new Variables(), closeAfter = false)
  }

  def runText(text: String, title: Option[String]): Unit = {
    hiveRunner.runText(text, title, variables = new Variables(), explainIfDryRun = true, closeAfter = false)
  }

  def testAll(fileIndex: FileIndex, context: FlamyContext): Unit = {
    hiveRunner.testAll(fileIndex, context)
  }

  def runPopulate(populateInfo: PopulateInfo): Unit = {
    hiveRunner.runPopulate(populateInfo)
  }

  def getInputsFromPopulate(populateInfo: PopulateInfo): Seq[Inputs] = {
    hiveRunner.getInputsFromPopulate(populateInfo)
  }

  def runCreateTable(create: TableFile): Unit = {
    hiveRunner.runCreateTable(create)
  }

  def runCreateSchema(schemaFile: SchemaFile): Unit = {
    hiveRunner.runCreateSchema(schemaFile)
  }

  def runCheckTable(create: TableFile): Unit = {
    hiveRunner.runCheckTable(create)
  }

  def runCheckSchema(schemaFile: SchemaFile): Unit = {
    hiveRunner.runCheckSchema(schemaFile)
  }

  /**
    * Should only be used to execute small queries.
    * @param text
    */
  def runText(text: String): Unit = {
    hiveRunner.runText(text, None, new Variables, explainIfDryRun = true)
  }

  /**
    * Should only be used to execute small queries, for experimental features
    * @param query
    */
  def executeQuery(query: String): StreamedResultSet = hiveRunner.executeQuery(query)

  def getConfiguration(key: String): Option[String] = hiveRunner.getConfiguration(key)

  def getStats: FileRunner#Stats = hiveRunner.getStats

  override def close(): Unit = hiveRunner.close()

  def interrupt(): Unit = hiveRunner.interrupt()

}

object FlamyRunner {

  @throws(classOf[FlamyException])
  def apply(context: FlamyContext): FlamyRunner = {
    try {
      new FlamyRunner(HiveRunner(context))
    }
    catch {
      case e: InterruptedException =>
        throw new FlamyException(e)
      case e: Throwable =>
        if(!Thread.currentThread().isInterrupted){
          e.printStackTrace()
        }
        throw new FlamyException(e)
    }
  }

}