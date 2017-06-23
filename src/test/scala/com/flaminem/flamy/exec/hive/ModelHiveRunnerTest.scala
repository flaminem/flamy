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
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.utils.logging.Logging
import org.scalatest.FreeSpec

/**
 * Created by fpin on 2/18/15.
 */
class ModelHiveRunnerTest extends FreeSpec with Logging{

  val context: FlamyContext =
    new FlamyContext(
      "flamy.model.dir.paths" -> "src/test/resources/test",
      "flamy.variables.path" -> "src/test/resources/test/VARIABLES.properties"
    )
  context.dryRun = true
  val runner = new ModelHiveRunner(context)

  "test removeLocation" in {
    val text: String =
      s"""CREATE DATABASE db1 LOCATION 'test';
         |CREATE DATABASE db2 location '/test/"\\'db2' ;
         |CREATE DATABASE db3 LOCATION \t\n\r "/test/\\"'db3"""".stripMargin
    val expected: String =
      """CREATE DATABASE db1 ;
        |CREATE DATABASE db2  ;
        |CREATE DATABASE db3 """.stripMargin
    val actual: String = ModelHiveRunner.removeLocation(text)
    assert(expected === actual)
  }


  "getConfiguration" - {
    "should work with hive config parameters" in {
      val key = "hive.support.concurrency"
      assert(runner.getConfiguration(key) === Some("false"))
    }

    "should work with undefined hive config parameters " in {
      val key = "hive.tata.toto"
      assert(runner.getConfiguration(key) === None)
    }
  }


  "A ModelHiveRunner should be able to run queries" ignore {
    val presets: String =
      """set hadoop.common.configuration.version=2.2.0 ;
        |set hive.debug.localtask=true ;
        |set hive.exec.mode.local.auto=true ;""".stripMargin
    val createDb1Query: String = "CREATE DATABASE db1"
    val createDb2Query: String = "CREATE DATABASE db2 LOCATION '/test/\"\\'db2'"
    val createDb3Query: String = "CREATE DATABASE db3 LOCATION \"/test/\\\"'db3\""
    val createDest: String = "CREATE TABLE db1.dest (col1 INT) STORED AS ORC"
    val createSource: String = "CREATE TABLE db2.source (a INT, b INT, c INT, d INT)"
    val createViewQuery: String =
      """CREATE VIEW IF NOT EXISTS db1.view AS
        |SELECT 'col1', 'col2', `T`.*
        |FROM (SELECT a+b as col1, c+d as col2 FROM db2.source) T""".stripMargin
    val populateQuery: String = "INSERT OVERWRITE TABLE db1.dest SELECT MAX(a+b) FROM db2.source"
    val cleanQuery: String =
      """DROP TABLE db1.dest ;
        |DROP VIEW db1.view ;
        |DROP TABLE db2.source ;
        |DROP DATABASE db1 ;
        |DROP DATABASE db2 ;
        |DROP DATABASE db3 ;""".stripMargin
    val context = new FlamyContext("test")
    context.dryRun = true
    val runner: ModelHiveRunner = new ModelHiveRunner(context)
    runner.runText(presets, None, new Variables, explainIfDryRun = false)
    runner.runCreate(createDb1Query, new Variables)
    runner.runCreate(createDb2Query, new Variables)
    runner.runCreate(createDb3Query, new Variables)
    runner.runCreate(createDest, new Variables)
    runner.runCreate(createSource, new Variables)
    runner.runCreate(createViewQuery, new Variables)
    runner.runCreate(createViewQuery, new Variables)
    runner.runText(populateQuery, None, new Variables, explainIfDryRun = true)
    runner.runText(cleanQuery, None, new Variables, explainIfDryRun = false)
    runner.close()
    assert(runner.getStats.getFailCount==0)
  }


  "A ModelHiveRunner should be able to run queries in parallel" ignore {
    val presets: String =
      """set hadoop.common.configuration.version=2.2.0 ;
        |set hive.debug.localtask=true ;
        |set hive.exec.mode.local.auto=true ;""".stripMargin
    val createDb1Query: String = "CREATE DATABASE db1"
    val createDb2Query: String = "CREATE DATABASE db2"
    val createSource: String = "CREATE TABLE db2.source (a INT, b INT, c INT, d INT)"
    val createDest1: String = "CREATE TABLE db1.dest1 (col1 INT) STORED AS ORC"
    val createDest2: String = "CREATE TABLE db1.dest2 (col1 INT) STORED AS ORC"
    val createDest3: String = "CREATE TABLE db1.dest3 (col1 INT) STORED AS ORC"
    val createDest4: String = "CREATE TABLE db1.dest4 (col1 INT) STORED AS ORC"
    val createDest5: String = "CREATE TABLE db1.dest5 (col1 INT) STORED AS ORC"
    val populateQuery1: String = "INSERT OVERWRITE TABLE db1.dest1 SELECT MAX(a+b) FROM db2.source ;"
    val populateQuery2: String = "INSERT OVERWRITE TABLE db1.dest2 SELECT MAX(a+b) FROM db2.source ;"
    val populateQuery3: String = "INSERT OVERWRITE TABLE db1.dest3 SELECT MAX(a+b) FROM db2.source ;"
    val populateQuery4: String = "INSERT OVERWRITE TABLE db1.dest4 SELECT MAX(a+b) FROM db2.source ;"
    val populateQuery5: String = "INSERT OVERWRITE TABLE db1.dest5 SELECT MAX(a+b) FROM db2.source ;"
    val cleanQuery: String =
      """DROP TABLE db1.dest1 ;
        |DROP TABLE db1.dest2 ;
        |DROP TABLE db1.dest3 ;
        |DROP TABLE db1.dest4 ;
        |DROP TABLE db1.dest5 ;
        |DROP TABLE db2.source ;
        |DROP DATABASE db1 ;
        |DROP DATABASE db2 ;""".stripMargin
    val runner: ModelHiveRunner = new ModelHiveRunner(context)
    runner.runText(presets, None, new Variables, explainIfDryRun = false)
    runner.runCreate(createDb1Query, new Variables)
    runner.runCreate(createDb2Query, new Variables)
    runner.runCreate(createDest1, new Variables)
    runner.runCreate(createDest2, new Variables)
    runner.runCreate(createDest3, new Variables)
    runner.runCreate(createDest4, new Variables)
    runner.runCreate(createDest5, new Variables)
    runner.runCreate(createSource, new Variables)
    logger.info("A")
    Seq(populateQuery1,populateQuery2).par.foreach{
      case query =>
        val r = new ModelHiveRunner(context)
        r.runText(query, None, new Variables, explainIfDryRun = true)
    }
    logger.info("B")
    runner.runText(cleanQuery, None, new Variables, explainIfDryRun = false)
    runner.close()
    assert(runner.getStats.getFailCount==0)
  }


}
