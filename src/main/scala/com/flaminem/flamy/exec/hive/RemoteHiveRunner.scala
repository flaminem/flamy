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

import java.sql._

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.utils.logging.Logging
import com.flaminem.flamy.utils.sql.ResultRow
import com.flaminem.flamy.utils.sql.hive.StreamedResultSet

object RemoteHiveRunner{
  private val HIVE_SERVER2_DRIVER: String = "org.apache.hive.jdbc.HiveDriver"

}

/**
 * HiveRunner used to run queries on Hive via a jdbc connection to a HiveServer2.
 */
class RemoteHiveRunner(override val context: FlamyContext) extends HiveRunner(context: FlamyContext) with Logging {
  import com.flaminem.flamy.exec.hive.RemoteHiveRunner._

  logger.info("Creating new RemoteHiveRunner: " + this)

  Class.forName(HIVE_SERVER2_DRIVER)

  private val host: String = context.getHiveServerUri
  private val login: String = context.getHiveServerLogin
  private val password: String = ""
  private val connectionString: String = "jdbc:hive2://" + host + "/default"

  private var _con: Option[Connection] = None
  private var _stmt: Option[Statement] = None

  def con: Connection = {
    if(_con.isEmpty) {
      _con = Some(DriverManager.getConnection(connectionString, login, password))
    }
    _con.get
  }

  def stmt: Statement = {
    if(_stmt.isEmpty) {
      _stmt = Some(con.createStatement())
    }
    _stmt.get
  }

  /**
    * Run a query and ignore the result.
    * @param query
    * @param title will be set as the name of the mapreduce job.
    * @return
    */
  protected override def runPreparedQuery(query: String, title: Option[String]): Int = {
    val buildQ = context.getVariables.replaceInText(query)
    if(title.isDefined){
      stmt.execute("SET mapreduce.job.name = " + title.get + "")
      stmt.execute("SET hive.query.name = " + title.get + "")
      stmt.execute("SET spark.job.description = " + title.get + "")
    }
    stmt.execute(buildQ)
    0
  }

  /**
    * Run a query and returns the result set.
    * @param query
    * @return
    */
  protected override def executePreparedQuery(query: String, title: Option[String]): StreamedResultSet = {
    import com.flaminem.flamy.utils.sql._
    stmt.executeQuery(query).stream
  }

  override def runCreate(text: String, variables: Variables): Unit = {
    runText(text, None, variables, explainIfDryRun = true)
  }

  override def runCheck(text: String, variables: Variables): Unit = {
    /* We do not execute check queries on prod (yet) */
  }

  /**
    * Depending on the HiveThriftServer2 implementation (spark's or hive's), the result does
    * not have the same format, so we try to recognize both.
    * @param res
    */
  private[hive] def readConfigurationResult(key: String, res: ResultRow): Option[String] = {
    res.metaData.columnNames match {
      case Seq("set") => readHiveConfigurationResult(key, res)
      case Seq("key", "value") => readSparkConfigurationResult(key, res)
    }
  }

  /**
    * If the value is defined, spark returns:
    * <pre>
    * +---------------------------+--------------------+--+
    * |            key            |        value       |
    * +---------------------------+--------------------+--+
    * | config.parameter.name     | config.param.value |
    * +---------------------------+--------------------+--+
    * </pre>
    * If it is not defined, it returns:
    * <pre>
    * +---------------------------+--------------+--+
    * |            key            |    value     |
    * +---------------------------+--------------+--+
    * | config.parameter.name     | &lt;undefined&gt;  |
    * +---------------------------+--------------+--+
    * </pre>
    */
  private def readSparkConfigurationResult(key: String, res: ResultRow): Option[String] = {
    res(1) match {
      case "<undefined>" => None
      case s => Some(s)
    }
  }

  /**
    * If the value is defined, spark returns:
    * <pre>
    * +------------------------------------------+--+
    * |                  set                     |
    * +------------------------------------------+--+
    * | config.parameter.name=config.param.value |
    * +------------------------------------------+--+
    * </pre>
    * If it is not defined, it returns:
    * <pre>
    * +------------------------------------+--+
    * |                  set               |
    * +------------------------------------+--+
    * | config.parameter.name is undefined |
    * +------------------------------------+--+
    * </pre>
    *
    * environment, system, hiveconf, hivevar, metaconf variables are not supported yet, since they behave differently.
    */
  private def readHiveConfigurationResult(key: String, res: ResultRow): Option[String] = {
    val regex = s"$key=(.*)".r
    regex.findFirstMatchIn(res(0)) match {
      case None => None
      case Some(m) => Some(m.group(1))
    }
  }

  /**
    * Retrieve the value of given configuration parameter.
    *
    * @param key
    * @return
    */
  def getConfiguration(key: String): Option[String] = {
    if(!key.matches("^[-_\\p{Alnum}.]+$")) {
      throw new IllegalArgumentException(s"$key is not a valid configuration parameter name")
    }
    val value: Option[String] =
      for{
        res: ResultRow <- executePreparedQuery(s"SET $key", None).toIterable.headOption
        conf <- readConfigurationResult(key, res)
      }  yield {
        conf
      }
    value
  }

  def interrupt(): Unit = {
    _stmt.foreach{_.cancel()}
  }

  def close(): Unit = {
    _stmt.foreach{_.close()}
    _stmt = None
    _con.foreach{_.close()}
    _con = None
  }

  override def toString: String = "RemoteHiveRunner [connectionString=" + connectionString + "]"

}
