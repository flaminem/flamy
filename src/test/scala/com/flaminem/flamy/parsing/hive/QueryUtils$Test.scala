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

import org.scalatest.{FreeSpec, Matchers}

/**
 * Created by fpin on 11/20/14.
 */
class QueryUtils$Test extends FreeSpec with Matchers {

  "cleanAndSplitQuery" - {

    "should work fine" in {
      val query: String =
        """-- DROP TABLE db1.source ;
          |CREATE TABLE db1.source (
          |  string_col STRING,
          |  int_col INT,
          |  time_col TIMESTAMP,
          |  time_col_2 timestamp,
          |  int_col_2 INT,
          |  string_col_2 string
          |)
          |STORED AS SEQUENCEFILE
          |""".stripMargin
      assert (1 === QueryUtils.cleanAndSplitQuery(query).size)
    }

    "should work with strings containing semicolons" in {
      val query: String = "INSERT OVERWRITE TABLE toto SELECT * FROM tata WHERE AI.day < '2';"
      assert(1 === QueryUtils.cleanAndSplitQuery(query).size)
      assert("INSERT OVERWRITE TABLE toto SELECT * FROM tata WHERE AI.day < '2'" === QueryUtils.cleanAndSplitQuery(query).iterator.next)
    }

    "comments with /*...*/ are not supported by Hive" in {
      val query: String = "/*hello, */World"
      assert(QueryUtils.cleanAndSplitQuery(query) === Seq("/*hello, */World"))
    }

    "comments after a query should be removed" in {
      val query: String =
        """CREATE DATABASE IF NOT EXISTS test ;
          |-- SHOW SCHEMAS""".stripMargin
      assert(QueryUtils.cleanAndSplitQuery(query) === Seq("CREATE DATABASE IF NOT EXISTS test"))
    }

  }

  "removeAnnotations" - {

    "should work fine" in {
      val query: String =
        s"""
          | SELECT col1, "@thisIsAFirstTrap(gotcha once)" ;
          |
          | @regen(
          |   IGNORE db1.source ;
          |   SELECT
          |     IF (
          |       `db2.source2.partitions`.part = "toto"
          |        , "tata"
          |        , "@thisIsATrap(gotcha)"
          |     )
          |   END as part
          |   ;
          | )
          |
          | INSERT OVERWRITE TABLE db2.dest PARTITION(part = $${partition:part})
          | SELECT col1, "@thisIsAnotherTrap(gotcha again)"
          | FROM db1.source1 S1
          | JOIN db2.source2 S2
          | ON S1.id = S2.id
          |
          |""".stripMargin
      val s = " "
      val expected =
        s"""
          | SELECT col1, "@thisIsAFirstTrap(gotcha once)" ;
          |
          |$s
          |
          | INSERT OVERWRITE TABLE db2.dest PARTITION(part = $${partition:part})
          | SELECT col1, "@thisIsAnotherTrap(gotcha again)"
          | FROM db1.source1 S1
          | JOIN db2.source2 S2
          | ON S1.id = S2.id
          |
          |""".stripMargin
      println(QueryUtils.removeAnnotations(query))
      assert( QueryUtils.removeAnnotations(query) === expected )
    }

  }

  "parseAnnotations" - {

    "should work fine" in {
      val query: String =
        s"""
           | SELECT col1, "@thisIsAFirstTrap(gotcha once)"
           |
           | @regen(IGNORE db1.source)
           | @regen2(
           |  SELECT
           |    IF (
           |       `db2.source2.partitions`.part = "toto"
           |        , "tata"
           |        , "@thisIsATrap(gotcha)"
           |    )
           |  END as part
           |   ;
           | )
           |
           | INSERT OVERWRITE TABLE db2.dest PARTITION(part = $${partition:part})
           | SELECT col1, "@thisIsAnotherTrap(gotcha again)"
           | FROM db1.source1 S1
           | JOIN db2.source2 S2
           | ON S1.id = S2.id
           |
           |""".stripMargin
      val expected =
        Seq(
          "regen" -> "IGNORE db1.source",
          "regen2" ->
            """
              |  SELECT
              |    IF (
              |       `db2.source2.partitions`.part = "toto"
              |        , "tata"
              |        , "@thisIsATrap(gotcha)"
              |    )
              |  END as part
              |   ;
              | """.stripMargin
        )
      assert( QueryUtils.findAnnotations(query) === expected )
    }

  }

}
