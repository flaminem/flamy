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

package com.flaminem.flamy.parsing.hive.ast

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.conf.hive.ModelHiveContext
import org.apache.hadoop.hive.ql.parse.{ASTNode, ParseDriver}
import org.scalatest.{FreeSpec, Matchers}

/**
  * Created by fpin on 9/4/16.
  */
class SQLFormatter$Test extends FreeSpec with Matchers {

  implicit val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")

  "treeToSQL should work with quoted names" in {
    val query =
      """INSERT OVERWRITE TABLE T1 PARTITION(day)
        |SELECT
        |  c1,
        |  c2
        |FROM db_source.`t1.t2`
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    println(SQLFormatter.treeToSQL(tree))
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with INSERT INTO" in {
    val query =
      """INSERT INTO TABLE T1 PARTITION(day)
        |SELECT
        |  c1,
        |  c2
        |FROM db_source.`t1.t2`
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with NULLs" in {
    val query =
      """SELECT
        |  NULL
        |FROM db_source
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  // TODO: test all operators in the doc https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
  "treeToSQL should work with simple expressions" in {
    val query =
      """SELECT
        |  1 + 2,
        |  1 - 2,
        |  - 3,
        |  2 * 2,
        |  2 / 2
        |FROM db_source
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with LIMIT" in {
    val query =
      """SELECT
        |  *
        |FROM (
        |  SELECT
        |    *
        |  FROM db_source
        |  LIMIT 10
        |) T
        |LIMIT 5
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with lateral views" in {

    val query =
      """INSERT OVERWRITE TABLE T1 PARTITION(day)
        |SELECT
        |  c1,
        |  c2
        |FROM T2
        |LATERAL VIEW explode(myMap) LV as c1, c2
        |LATERAL VIEW OUTER explode(myMap) LV as c1, c2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with AND" in {
    val query =
      """INSERT OVERWRITE TABLE T1 PARTITION(day)
        |SELECT
        |  c1 AND c2
        |FROM T2
        |WHERE c1 = c2
        |  AND c1 = c2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with OR" in {
    val query =
      """INSERT OVERWRITE TABLE T1 PARTITION(day = "2016-01-01")
        |SELECT
        |  c1 OR c2
        |FROM T2
        |WHERE c1 = c2
        |   OR c1 = c2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with multi-insert" in {
    val query =
      """FROM T2
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c1")
        |SELECT
        |  c1
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c2")
        |SELECT
        |  c2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with multi-insert and CTEs" in {
    val query =
      """WITH T2 AS (
        |  SELECT
        |    *
        |  FROM T
        |)
        |FROM T2
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c1")
        |SELECT
        |  c1
        |LIMIT 1
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c2")
        |SELECT
        |  c2
        |LIMIT 1
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with multi-insert and splitMultiInsert is true" in {
    val query =
      """WITH T2 AS (
        |  SELECT
        |    *
        |  FROM T
        |)
        |FROM T2
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c1")
        |SELECT
        |  c1
        |WHERE c1 = 1
        |LIMIT 1
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c2")
        |SELECT
        |  c2
        |WHERE c1 = 1
        |LIMIT 1
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    val expected =
      """WITH T2 AS (
        |  SELECT
        |    *
        |  FROM T
        |)
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c1")
        |SELECT
        |  c1
        |FROM T2
        |WHERE c1 = 1
        |LIMIT 1
        |;
        |WITH T2 AS (
        |  SELECT
        |    *
        |  FROM T
        |)
        |INSERT OVERWRITE TABLE T1 PARTITION(part = "c2")
        |SELECT
        |  c2
        |FROM T2
        |WHERE c1 = 1
        |LIMIT 1
        |""".stripMargin

    assert(SQLFormatter.treeToSQL(tree, splitMultiInsert = true) === expected)
  }

  "treeToSQL should work with constants" in {
    val query =
      """SELECT
        |  true,
        |  false,
        |  1,
        |  "a",
        |  '1'
        |""".stripMargin
    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with SELECT DISTINCT" in {
    val query =
      """SELECT DISTINCT
        |  c1,
        |  c2
        |""".stripMargin
    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with the MINUS prefix" in {
    val query =
      """SELECT
        |  c1,
        |  - c2
        |""".stripMargin
    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with complex types" in {
    val query =
      """SELECT
        |  mymap["a"],
        |  very.big.col
        |""".stripMargin
    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with joins" in {
    val query =
      """INSERT OVERWRITE TABLE T1 PARTITION(day)
        |SELECT
        |  c1,
        |  c2
        |FROM T2
        |LEFT JOIN T3
        |  ON T3.c3 = T2.c3 AND 1 = 1 AND 2 > 1
        |RIGHT JOIN T4
        |  ON T4.c3 = T2.c3
        |CROSS JOIN T5
        |LEFT SEMI JOIN T6
        |  ON T6.c6 = T5.c5
        |JOIN T7
        |JOIN T8
        |  ON T8.c3 = T2.c3
        |WHERE c1 + c2 = c2 - c3
        |  AND (c1 > c2 OR c1 >= c2)
        |  AND (c1 <> c2 OR c2 < c1)
        |  AND c1 <= c2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with subqueries" in {

    val query =
      """WITH T1 AS (
        |  SELECT
        |    *
        |  FROM db2.source
        |)
        |, T2 AS (
        |  SELECT
        |    T.*
        |  FROM db2.source T
        |)
        |INSERT OVERWRITE TABLE db1.dest PARTITION(day)
        |SELECT
        |  c1
        |FROM (
        |  SELECT
        |    c1
        |  FROM T2
        |) T3
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }


  "treeToSQL should work with functions" in {
    val query =
      """SELECT
        |  c1 IS NULL,
        |  c2 IS NOT NULL,
        |  c3 LIKE "abc%",
        |  c4 RLIKE "abc.*",
        |  INT(c5) as c5,
        |  IF(c6, 1, 2) as c6,
        |  COUNT(DISTINCT c7) as c7,
        |  COUNT(*) as c8,
        |  CASE c9
        |    WHEN 1 THEN 1
        |    ELSE 2
        |  END as c9,
        |  CASE
        |    WHEN c10 = 1 THEN 1
        |    WHEN c10 = 2 THEN 2
        |    ELSE 3
        |  END as c10,
        |  c1 IN (c2, c3),
        |  NOT c1 IN (c2, c3),
        |  b BETWEEN a AND c,
        |  b NOT BETWEEN a AND c
        |FROM db2.source
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }


  "treeToSQL should work with GROUP BY, HAVING, ORDER BY" in {

    val query =
      """SELECT
        |  c1,
        |  c2,
        |  SUM(c3) as c3
        |FROM db2.source
        |GROUP BY c1, c2
        |HAVING c2 > 0
        |ORDER BY c1, c2 DESC
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }


  "treeToSQL should work with DISTRIBUTE BY, SORT BY" in {
    val query =
      """SELECT
        |  c1,
        |  c2,
        |  c3
        |FROM db2.source
        |DISTRIBUTE BY c1, c2
        |SORT BY c1, c2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with CLUSTER BY" in {
    val query =
      """SELECT
        |  c1,
        |  c2,
        |  c3
        |FROM db2.source
        |CLUSTER BY c1, c2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with WHERE IN" in {
    val query =
      """SELECT
        |  c1
        |FROM db2.source1 T
        |WHERE T.c1 IN (
        |  SELECT
        |    c1
        |  FROM db2.source2
        |)
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with WHERE NOT IN" in {
    val query =
      """SELECT
        |  c1
        |FROM db2.source1 T
        |WHERE NOT T.c1 IN (
        |  SELECT
        |    c1
        |  FROM db2.source2
        |)
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with WHERE EXISTS" in {
    val query =
      """SELECT
        |  c1
        |FROM db2.source1 T1
        |WHERE EXISTS (
        |  SELECT
        |    c1
        |  FROM db2.source2 T2
        |  WHERE T2.c1 = T1.c1
        |)
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "treeToSQL should work with WHERE NOT EXISTS" in {
    val query =
      """SELECT
        |  c1
        |FROM db2.source1 T1
        |WHERE NOT EXISTS (
        |  SELECT
        |    c1
        |  FROM db2.source2 T2
        |  WHERE T2.c1 = T1.c1
        |)
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)
    assert(SQLFormatter.treeToSQL(tree) === query)
  }

  "indentation should be correct" in {
    val query =
      """SELECT
        |  T1.p3 as day
        |FROM (
        |  SELECT
        |    p1 as p3
        |  FROM (
        |    SELECT
        |      T1.p1,
        |      T1.p2
        |    FROM `db2.source1.partitions` T1
        |    JOIN `db2.source1.partitions` T2
        |  ) view
        |) T1
        |JOIN `db2.source2.partitions` T2
        |""".stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(context).hiveContext)

    assert(SQLFormatter.treeToSQL(tree) === query)
  }


}
