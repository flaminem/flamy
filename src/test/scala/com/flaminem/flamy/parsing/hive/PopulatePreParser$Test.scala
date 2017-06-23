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

import com.flaminem.flamy.parsing.model.MergeableTableDependencyCollection
import org.scalatest.FlatSpec

/**
 * Created by fpin on 11/20/14.
 */
class PopulatePreParser$Test extends FlatSpec {

  def testParseQuery(text: String, expected: String) {
    val actual: MergeableTableDependencyCollection = PopulatePreParser.parseText(text)
    if (expected != null) {
      assert(actual.toString === expected)
    }
    else {
      System.out.println(actual.toString)
    }
  }

  def testFailure(query: String) {
    intercept[FlamyParsingException]{
      PopulatePreParser.parseText(query)
    }
  }

  "afterParenthesisSourceRE" should "correctly recognise strings" in {
    val regex = PopulatePreParser.afterParenthesisSourceRE
    assert(regex.findFirstMatchIn("  AS T2, db3.tutu").nonEmpty)
  }


  "PopulatePreParser" should "correctly parse a LOAD DATA query" in {
    val query: String = "LOAD DATA INPATH '' OVERWRITE INTO TABLE db1.test PARTITION (day=\'\')"
    val expectedJSON: String = """[{"tableType":"TABLE","tableName":"test","schemaName":"db1"}]"""
    val expected : String = "[db1.test]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a SET query" in {
    val query: String = "SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"
    val expected : String = "[]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with weird spacing" in {
    val query =
      """INSERT
        |     OVERWRITE
        |       TABLE
        |          db1.dest
        |            SELECT
        |    col1
        |    FROM              db2.source""".stripMargin
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with lowercase" in {
    val query = "insert overwrite table db1.dest select col1 from db2.source"
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with weird table alias" in {
    val query = "insert overwrite table db1.dest select col1 from db2.source1 `this is a weird table alias`, db2.source2"
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1, db2.source2])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a simple query 1" in {
    val query = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.source"
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a simple query 2" in {
    val query = "INSERT OVERWRITE TABLE db1.dest SELECT t1.col1 FROM db2.source T1"
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse queries with joins" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM db2.tutu AS T2
        | JOIN db3.tutu AS T3
        | ON T3.id=T2.id
        | LEFT JOIN db4.tutu AS T4
        | ON T4.id=T2.id
        | RIGHT JOIN db5.tutu AS T5
        | ON T5.id=T2.id
        | FULL JOIN db6.tutu AS T6
        | ON T6.id=T2.id
        | LEFT OUTER JOIN db7.tutu AS T7
        | ON T7.id=T2.id
        | RIGHT OUTER JOIN db8.tutu AS T8
        | ON T8.id=T2.id
        | FULL OUTER JOIN db9.tutu AS T9
        | ON T9.id=T2.id
        | LEFT SEMI JOIN db10.tutu AS T10
        | ON (T10.id=T2.id)
        | CROSS JOIN db11.tutu AS T11
        | ON T11.id=T2.id
        | """.stripMargin
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db10.tutu, db11.tutu, db2.tutu, db3.tutu, db4.tutu, db5.tutu, db6.tutu, db7.tutu, db8.tutu, db9.tutu])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse queries with comma for CROSS JOINS" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM db2.tutu, db3.tutu
        | """.stripMargin
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tutu, db3.tutu])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse queries with comma for CROSS JOINS 2" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM (SELECT * FROM db2.tutu) T2, db3.tutu
        | """.stripMargin
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tutu, db3.tutu])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse queries with comma for CROSS JOINS 3" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM (SELECT * FROM db2.tutu) AS T2, db3.tutu
        | """.stripMargin
    val expected: String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tutu, db3.tutu])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse queries with comma for CROSS JOINS 4" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM (SELECT * FROM db2.tutu)AS `T2` , db3.tutu
        | """.stripMargin
    val expectedJSON: String = """[{"tableDeps":[{"tableType":"REF","tableName":"tutu","schemaName":"db2"},{"tableType":"REF","tableName":"tutu","schemaName":"db3"}],"tableType":"TABLE","tableName":"toto","schemaName":"db1"}]"""
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tutu, db3.tutu])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse queries with no space !!!" in {
    val query: String =
      """ WITH`T1`AS(SELECT`c1`FROM`db2.toto`JOIN`db3`.`toto`,db4.toto`t2`,db5.toto)
        | INSERT OVERWRITE TABLE`db1.Toto`SELECT`colA`FROM`t1`
        | """.stripMargin
    val expected: String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.toto, db3.toto, db4.toto, db5.toto])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with join 1" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM db2.tata AS T1
        | JOIN db3.tutu AS T2
        | ON T1.id=T2.id
        | WHERE T1.day < '2'
        | """.stripMargin
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tata, db3.tutu])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with join 2" in {
    val query =
      """ INSERT OVERWRITE TABLE db1.toto
        | SELECT colA, colB
        | FROM
        | (
        |   FROM
        |   (
        |     SELECT tata.a as cA, tata.b
        |     FROM db2.tata
        |   ) `T1`
        |   SELECT T1.a as colA, b as colB
        | ) `T2`
        | JOIN db3.tutu `T3`
        | ON T3.a=T3.a
        | WHERE T3.day BETWEEN date_sub('2',2) AND '2'
        | """.stripMargin
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tata, db3.tutu])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with WHERE col IN (subquery)" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT a FROM db2.source1
        | WHERE b IN (SELECT c FROM db2.source2)
        | """.stripMargin
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1, db2.source2])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with WHERE EXISTS (subquery)" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT a FROM db2.source1
        | WHERE EXISTS (SELECT c FROM db2.source2)
        | """.stripMargin
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1, db2.source2])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse query with partition" in {
    val query: String = "INSERT OVERWRITE TABLE db1.Toto PARTITION(coco='hello') SELECT colA FROM db2.tata AS T1"
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tata])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse text with multiple queries" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.source1 ; INSERT INTO TABLE db1.dest SELECT col1 FROM db2.source2"
    val expected : String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1, db2.source2])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse a query with column alias in subquery" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT num FROM (SELECT COUNT(1) as num FROM db2.source) T"
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse a query with column alias in subquery (bis)" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT T.c FROM (SELECT source.col1 as c FROM db2.Source) T"
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(text, expected)
  }

  it should "When querying on a struct object eg (very.big.object) the dependency column is very.big.object and the name given by Hive to the column will be object" in {
    val query: String = "INSERT OVERWRITE TABLE db1.dest SELECT very.big.object FROM db2.source T"
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(query, expected)
  }

  it should "As of Hive 0.12, the expected behavior here is to accept this syntax and ignore the 'as col1', for now the expected behavior with flamy is the same, but perhaps we should raise an error here" in {
    val query: String = "CREATE VIEW db2.view AS SELECT * as col1 FROM db1.source"
    val expected: String = "[TableDependency(type=VIEW, name=view, schema=db2, tableDeps[db1.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a query with GROUP BY on computed column" in {
    val query: String = "INSERT OVERWRITE TABLE db1.dest SELECT f(col) as fc, count(*) FROM db2.src GROUP BY fc ORDER BY fc"
    val expected: String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.src])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse a regular query" in {
    val text: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT colA, colB  FROM (
        |     FROM (SELECT tata.a as cA, tata.b FROM db2.src1) `T1`
        |     SELECT T1.a as colA, b as colB
        | ) `T2`
        | JOIN db2.src2 `T3` ON T3.a=T3.a
        | WHERE T3.day BETWEEN date_sub('2',2) AND '2'""".stripMargin
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.src1, db2.src2])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse bug 1" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT num FROM (SELECT COUNT(1) as num FROM db2.source) T"
    val expectedJSON: String = """[{"tableDeps":[{"tableType":"REF","tableName":"source","schemaName":"db2"}],"tableType":"TABLE","tableName":"dest","schemaName":"db1"}]"""
    val expected : String = null
    testParseQuery(text, expected)
  }

  // When querying on a struct object eg (very.big.object) the dependency column is very.big.object and the name given by Hive to the column will be object.
  it should "correctly parse StructColumns" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT very.big.object FROM db2.source T"
    val expected : String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(text, expected)
  }

  // As of Hive 0.12, the expected behavior here is to accept this syntax and ignore the "as col1"
  // For now the expected behavior with flamy is the same, but perhaps we should raise an error here
  it should "[weird] correctly parse VIEW with SELECT * as" in {
    val text: String = "CREATE VIEW db2.view AS SELECT * as col1 FROM db1.source"
    val expected : String = "[TableDependency(type=VIEW, name=view, schema=db2, tableDeps[db1.source])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse CREATE VIEW" in {
    val text: String =
      """ CREATE VIEW test.view AS
        | SELECT MAX(c1) as col1, MAX(c2), T.c2, c3
        | FROM (SELECT a+b as c1, c+d as c2, e+f as c3 FROM db.source) T
        | """.stripMargin
    val expected : String = "[TableDependency(type=VIEW, name=view, schema=test, tableDeps[db.source])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse CREATE VIEW ... AS SELECT *" in {
    val query: String = "CREATE VIEW test.view AS SELECT 'a', *, 'b', `T`.*  FROM (SELECT a+b as col1, c+d as col2 FROM db.source) T"
    val expected : String = "[TableDependency(type=VIEW, name=view, schema=test, tableDeps[db.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse CREATE VIEW IF NOT EXISTS" in {
    val query: String = "CREATE VIEW IF NOT EXISTS test.view AS " + "SELECT MAX(c1) as col1, MAX(c2), T.c2, c3 " + "FROM (SELECT a+b as c1, c+d as c2, e+f as c3 FROM db.source) T"
    val expected : String = "[TableDependency(type=VIEW, name=view, schema=test, tableDeps[db.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse CREATE VIEW IF NOT EXISTS ... AS SELECT *" in {
    val query: String = "CREATE VIEW IF NOT EXISTS test.view AS  SELECT 'a', *, 'b', `T`.* FROM (SELECT a+b as col1, c+d as col2 FROM db.source) T"
    val expectedJSON: String = """[{"tableDeps":[{"tableType":"REF","tableName":"source","schemaName":"db"}],"tableType":"VIEW","tableName":"view","schemaName":"test"}]"""
    val expected : String = "[TableDependency(type=VIEW, name=view, schema=test, tableDeps[db.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse with sub-query" in {
    val query = "INSERT OVERWRITE TABLE db1.toto SELECT colA, colB FROM (SELECT tata.a as cA, tata.b FROM db2.tata) T1"
    val expected : String = "[TableDependency(type=TABLE, name=toto, schema=db1, tableDeps[db2.tata])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse with MapReduce" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | REDUCE * using ''
        | AS colA, colB, colC
        | FROM (SELECT * FROM db2.source) mapped ;""".stripMargin
    val expectedJSON = """[{"tableDeps":[{"tableType":"REF","tableName":"source","schemaName":"db2"}],"tableType":"TABLE","tableName":"temp_id_red_result","schemaName":"dbm_reports_temp"}]"""
    val expected : String = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source])]"
    testParseQuery(query, expected)
  }

  it should "correctly parse queries with one CTE" in {
    val text: String =
      """ WITH T AS (SELECT col1 FROM db2.source1)
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col2 FROM T""".stripMargin
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse queries with multiple CTEs" in {
    val text: String =
      """ WITH
        | T1 AS (
        |   SELECT col1, col2 FROM db2.source1
        | ),
        | T2 AS (
        |   SELECT col1, col2 FROM T1
        | ),
        | T3 as (
        |   SELECT col1, col2 as `"(it's a trap!)"` FROM T2
        | )
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col2 FROM T3""".stripMargin
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1])]"
    testParseQuery(text, expected)
  }

  it should "fail with wrong CTE name" in {
    val text: String =
      """ WITH T1 AS(
        | SELECT col1 FROM db2.source1
        |),
        |T2 AS (
        | SELECT col1 FROM db2.source2
        |)
        |, T3 as(
        | SELECT col1, col2 as "(it's a trap!)" FROM db2.source3
        |)
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col2 FROM T""".stripMargin
    testFailure(text)
  }

  it should "correctly parse queries with weird CTE name" in {
    val text: String =
      """ WITH `THIS IS A WEIRD TABLE NAME :-(` AS(
        | SELECT col1 FROM db2.source1
        |)
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col2 FROM `this is a weird table name :-(` , db2.source2
        | """.stripMargin
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1, db2.source2])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse queries with really awful CTE name" in {
    val text: String =
      """ WITH `OMG this is an awful table name :-( WITH sql_injection AS (select and_everything, 'drop table' from db2.source1)` AS(
        | SELECT col1 FROM db2.source1
        |)
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col2 FROM `OMG this is an awful table name :-( WITH sql_injection AS (select and_everything, 'drop table' from db2.source1)`""".stripMargin
    testParseQuery(text, null)
  }

  it should "correctly parse queries with trapped strings" in {
    val text: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT "FROM db2.source2" FROM db2.source1""".stripMargin
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse queries with trapped strings with backslashes" in {
    val text: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT "FROM db2.source2\\" FROM db2.source1""".stripMargin
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1])]"
    testParseQuery(text, expected)
  }

  it should "correctly parse queries with UNION ALL" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM
        |(
        |SELECT col1 FROM db2.source1
        |UNION ALL
        |SELECT col1 FROM db2.source2
        |UNION ALL
        |SELECT col1 FROM db2.source3
        |) T""".stripMargin
    val expected = "[TableDependency(type=TABLE, name=dest, schema=db1, tableDeps[db2.source1, db2.source2, db2.source3])]"
    testParseQuery(text, expected)
  }

  "emptyStrings" should "work correctly" in {
    assert(PopulatePreParser.emptyStrings(""""this is a string"""") === "\"\"")
    assert(PopulatePreParser.emptyStrings("""'this is also a string'""") === "''")
    assert(PopulatePreParser.emptyStrings(""""this 'one' looks \"weird\" but it is \'definitely\' a string"""") === "\"\"")
    val text: String =
      """  CREATE VIEW db1.dest AS
        |    SELECT
        |  CASE
        |    WHEN col1 = 0
        |    THEN "zero"
        |    WHEN COALESCE(length(col2),0)>0
        |    THEN "nonempty"
        |    WHEN (col3 LIKE "%toto-tata%" OR col3 LIKE "%foo-bar%" OR col3 LIKE "%titi%") AND col4 NOT LIKE "tutu_%"
        |    THEN "toto_like"
        |    ELSE NULL
        |  END as supercase,
        |  T.*
        |  FROM db2.source T
        |""".stripMargin
    val expected =
      """  CREATE VIEW db1.dest AS
        |    SELECT
        |  CASE
        |    WHEN col1 = 0
        |    THEN ""
        |    WHEN COALESCE(length(col2),0)>0
        |    THEN ""
        |    WHEN (col3 LIKE "" OR col3 LIKE "" OR col3 LIKE "") AND col4 NOT LIKE ""
        |    THEN ""
        |    ELSE NULL
        |  END as supercase,
        |  T.*
        |  FROM db2.source T
        |""".stripMargin
    assert(PopulatePreParser.emptyStrings(text) === expected)

  }

}
