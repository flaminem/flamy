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

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.conf.hive.ModelHiveContext
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.model.exceptions.FailedQueryException
import com.flaminem.flamy.model.files.PresetsFile
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.sql.hive.StreamedResultSet
import org.apache.hadoop.hive.cli.CliDriver
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState

/**
 * Created by fpin on 2/17/15.
 */
class ModelHiveRunner(override val context: FlamyContext) extends HiveRunner(context: FlamyContext) with Logging {

  private val modelHiveContext: ModelHiveContext = ModelHiveContext.getHeavyContext(context)
  assert(Option(SessionState.get()).isDefined)
  private val cliDriver = new CliDriver

  context.getHadoopBinPath match {
    case None => ()
    case Some(path) => cliDriver.processCmd(s"SET hadoop.bin.path=$path")
  }

  protected override def runPreparedQuery(query: String, title: Option[String]): Int = {
    if (query.toUpperCase.startsWith("LOAD DATA") || query.toUpperCase.startsWith("EXPLAIN LOAD DATA")) {
      logger.info("Skipping query: LOAD DATA")
      0
    }
    else {
      System.out.println(query)
      cliDriver.processCmd(query)
    }
  }

  /**
    * This implementation currently only works for regular queries. Other kind of commands (SET etc. will fail)
    * @param query
    * @param title title of the query. May be use to display information
    * @return
    */
  protected override def executePreparedQuery(query: String, title: Option[String]): StreamedResultSet = {
    val driver = new Driver
    val response = driver.run(query)
    if(response.getResponseCode > 0){
      throw new FailedQueryException()
    }
    import com.flaminem.flamy.utils.sql.hive.DriverExtension
    driver.stream
  }

  override def runCreate(text: String, variables: Variables): Unit = runText(ModelHiveRunner.removeLocation(text), None, variables, explainIfDryRun = false)

  override def runCheck(text: String, variables: Variables): Unit = runCreate(text: String, variables: Variables)

  private def addUDFJars(): Unit = {
    context.getUdfJarPaths.foreach{
      path => runPreparedQuery(s"ADD JAR $path", None)
    }
  }

  override def runPresets(file: PresetsFile): Unit = {
    addUDFJars()
    super.runPresets(file)
  }

  /**
    * Retrieve the value of given configuration parameter.
    *
    * @param key
    * @return
    */
  def getConfiguration(key: String): Option[String] = {
    val conf: HiveConf = modelHiveContext.hiveConf
    for {
      value <- Option(conf.get(key))
    } yield {
      value
    }
  }

  override def close(): Unit = () //sessionState.close()

  override def interrupt(): Unit = () //sessionState.close()

}

object ModelHiveRunner {

  private[hive] def removeLocation(text: String): String = {
    text.replaceAll("""(?i)location\s+(['"])([^\\]|\\.)*?\1""", "")
  }

}