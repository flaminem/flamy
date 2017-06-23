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

package com.flaminem.flamy.parsing.hive

import com.flaminem.flamy.conf.FlamyContext
import org.scalatest.FreeSpec

class CreateTableParser$Test extends FreeSpec {

  implicit val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")


  "a correct CREATE TABLE query should be correctly parsed" in {
    val query = "CREATE TABLE toto.test_table (id INT) PARTITIONED BY (week STRING)"
    val expectedResult = """Table(type=TABLE, name=test_table, schema=toto, columns[Column(name=id, type=int)], partitions[Column(name=week, type=string)])"""
    val actualResult = CreateTableParser.parseQuery(query)
    assert(actualResult.toString === expectedResult )
  }

  "a correct CREATE TABLE IF NOT EXISTS query should be correctly parsed" in {
    val query = "CREATE TABLE IF NOT EXISTS toto.test_table (id INT) PARTITIONED BY (week STRING)"
    val expectedResult = """Table(type=TABLE, name=test_table, schema=toto, columns[Column(name=id, type=int)], partitions[Column(name=week, type=string)])"""
    val actualResult = CreateTableParser.parseQuery(query)
    assert(actualResult.toString === expectedResult )
  }

  "a correct text of multiple queries should be correctly parsed" in {
    val text =
      """ -- DROP TABLE IF EXISTS DBM_reports.report ;
        | CREATE TABLE IF NOT EXISTS DBM_reports.report
        | (device_type INT,
        | mobile_make_id INT,
        | mobile_model_id INT
        | )
        | PARTITIONED BY (day STRING)
        | STORED AS SEQUENCEFILE ;
        | """.stripMargin
    val expectedResult = """Table(type=TABLE, name=report, schema=dbm_reports, columns[Column(name=device_type, type=int), Column(name=mobile_make_id, type=int), Column(name=mobile_model_id, type=int)], partitions[Column(name=day, type=string)])"""
    val actualResult = CreateTableParser.parseText(text)
    assert(actualResult.toString === expectedResult )
  }

}