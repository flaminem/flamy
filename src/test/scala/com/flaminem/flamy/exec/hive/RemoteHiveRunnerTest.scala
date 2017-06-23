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

import com.flaminem.flamy.conf.{Environment, FlamyContext, FlamyGlobalOptions}
import com.flaminem.flamy.model.{Column, Variables}
import com.flaminem.flamy.utils.sql.{MetaData, ResultRow}
import org.scalatest.{FreeSpec, Matchers}

/**
 * Created by fpin on 2/18/15.
 */
class RemoteHiveRunnerTest extends FreeSpec with Matchers {

  val context =
    new FlamyContext(
      new FlamyGlobalOptions(
        conf = Map(
          "flamy.environments" -> "remote",
          "flamy.model.dir.paths" -> "src/test/resources/test",
          "flamy.variables.path" -> "${flamy.model.dir.paths}/VARIABLES.properties",
          "flamy.env.model.hive.presets.path" -> "${flamy.model.dir.paths}/PRESETS.hql",
          "flamy.env.remote.hive.server.uri" -> "\"hmaster02.dc3.dataminem.net:10000\""
        )
      )
      , Some(Environment("remote"))
    )
  context.dryRun = true
  val runner = new RemoteHiveRunner(context)

  "testRemote1" ignore {
    val createDB: String = "CREATE DATABASE IF NOT EXISTS test"
    val createSource: String = "CREATE TABLE IF NOT EXISTS test.source (a INT, b INT, c INT, d INT)"
    val populate: String = "SELECT to_date(truncate_date(from_unixtime(unix_timestamp()),'WEEK')) FROM test.source"

    val runner: HiveRunner = new RemoteHiveRunner(context)
    runner.runCreate(createDB, new Variables)
    runner.runCreate(createSource, new Variables)
    runner.runText(populate, None, new Variables, explainIfDryRun = true)
  }

  "testRemote2" ignore {
    try {
      val context: FlamyContext = new FlamyContext("test", Environment("prod"))
      val runner: RemoteHiveRunner = new RemoteHiveRunner(context)
//      val con: Connection = runner.con
//      val st: Statement = con.createStatement
//      st.execute("USE dev_client_value")
//      st.execute("MSCK REPAIR TABLE url_semantic_data_s3")
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
  }

  "getConfiguration" - {
    "should work with hive config result" in {
      val key = "hive.support.concurrency"
      val value = "true"
      val metaData = new MetaData(new Column("set")::Nil)
      val res = new ResultRow(metaData, Seq(s"$key=$value"))
      assert(runner.readConfigurationResult(key, res) === Some(value))
    }

    "should work with undefined hive config result " in {
      val key = "hive.support.concurrency"
      val col = new Column("set")
      val res = new ResultRow(new MetaData(new Column("set")::Nil), Seq(s"$key is undefined"))
      assert(runner.readConfigurationResult(key, res) === None)
    }

    "should work with spark config result" in {
      val key = "hive.support.concurrency"
      val value = "true"
      val metaData = new MetaData(new Column("key")::new Column("value")::Nil)
      val res = new ResultRow(metaData, Seq(key, value))
      assert(runner.readConfigurationResult(key, res) === Some(value))
    }

    "should work with undefined spark config result " in {
      val key = "hive.support.concurrency"
      val metaData = new MetaData(new Column("key")::new Column("value")::Nil)
      val res = new ResultRow(metaData, Seq(key, "<undefined>"))
      assert(runner.readConfigurationResult(key, res) === None)
    }
  }


}
