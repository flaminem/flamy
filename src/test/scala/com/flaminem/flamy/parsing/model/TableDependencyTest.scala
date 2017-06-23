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

package com.flaminem.flamy.parsing.model

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.columns.ColumnValue
import com.flaminem.flamy.model.core.CompleteModelFactory
import com.flaminem.flamy.model.files.TableFile
import com.flaminem.flamy.model.{DummyTableFile, Variables}
import org.scalatest.FreeSpec

import scala.language.implicitConversions

class TableDependencyTest extends FreeSpec {

  /**
    * We allow implicit casting of text into DummyTableFiles
    * @param text
    * @return
    */
  implicit def stringToTableFile(text: String): TableFile = {
    DummyTableFile(text)
  }

  implicit def stringToOption(s: String): Option[String] = Option(s)

  implicit val flamyContext = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/empty_test")
  val tableGraph = TableGraph(flamyContext, Nil, checkNoMissingTable = false)

  def testSuccess(
    creates: Seq[String] = Nil,
    views: Seq[String] = Nil,
    populates: Seq[String] = Nil,
    variables: Variables = new Variables(),
    expected: Option[String] = None
  ): Unit = {
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    creates.foreach{modelFactory.analyzeCreate(_)}
    views.foreach{modelFactory.analyzeView(_)}
    val tableDeps: Seq[TableDependency] =
      populates.flatMap{modelFactory.analyzePopulateDependencies(_, isView = false, variables)}
    expected match {
      case Some(e) =>
        val td: TableDependency = tableDeps.last
        assert(td.toString === e)
      case None =>
        tableDeps.foreach{println}
    }
  }

  def testFailure(
    creates: Seq[String],
    views: Seq[String],
    populates: Seq[String],
    variables: Variables = new Variables()
  ) {
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    creates.foreach{modelFactory.analyzeCreate(_)}
    intercept[Exception] {
      views.foreach{modelFactory.analyzeView(_)}
      populates.foreach{modelFactory.analyzePopulate(_, variables)}
    }
  }

  "a simple query should succeed" in {
      val createQuery = "CREATE TABLE db2.source (col1 INT)"
      val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT t1.col1 FROM db2.Source T1"
      val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[db2.source.col1])"
      testSuccess(
        creates = createQuery :: Nil,
        populates = populateQuery :: Nil,
        expected = expected
      )
    }

  "a simple query with a partition variable should succeed" in {
    /* The bug here was that the Analyzer tries to resolve the ColumnValue part1 in a context where db2.source is not visible */
    val createQuery = "CREATE TABLE db2.source (col1 INT) PARTITIONED BY (part1 STRING)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT ${partition:part1} as col1
        |FROM (
        |   SELECT
        |    col1
        |   FROM db2.source
        |) T1 """.stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[db2.source.col1])"
    val vars = new Variables()
    vars += "partition:part1" -> "'${partition:part1}'"
    testSuccess(
      creates = createQuery :: Nil,
      populates = populateQuery :: Nil,
      variables = vars,
      expected = None
    )
  }

  "a query with an aliased column in a subquery with a star should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT T.col1 as col2 FROM (
        |  SELECT * FROM db2.source
        |) T
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col2")
    assert(tableDeps.head.columns.head.value === new ColumnDependency("col1", "db2", "source"))
  }

  "a query with an aliased column in a subquery with a prefixed star should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT T.col1 as col2 FROM (
        |  SELECT T.* FROM db2.source T
        |) T
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col2")
    assert(tableDeps.head.columns.head.value === new ColumnDependency("col1", "db2", "source"))
  }

  "a query with an aliased column in two nested subqueries with a start should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT T.col1 as col2
        |FROM (
        |  SELECT * FROM (
        |    SELECT col1 FROM db2.source
        |  ) T
        |) T
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col2")
    assert(tableDeps.head.columns.head.value === new ColumnDependency("col1", "db2", "source"))
  }

  "a query with an aliased column in a subquery should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT T.col1 as col2
        |FROM (
        |  SELECT col1 FROM db2.source
        |) T
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col2")
    assert(tableDeps.head.columns.head.value === new ColumnDependency("col1", "db2", "source"))
  }

  "a query with an aliased column in a subquery with a union should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT T.col1 as col2
        |FROM (
        |  SELECT * FROM db2.source
        |) T
        |UNION ALL
        |SELECT col1 as col2 FROM db2.source
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col2")
    assert(tableDeps.head.columns.head.value === new ColumnDependency("col1", "db2", "source"))
  }

  "a query with a constant in a subquery should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT T.col1 as col2
        |FROM (
        |  SELECT 1 as col1 FROM db2.source
        |) T
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col2")
    assert(tableDeps.head.columns.head.value === ColumnValue("1"))
  }

  "a query with an aliased column in a view should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val viewQuery =
      """CREATE VIEW db2.view AS
        |SELECT col1 as col2 FROM db2.source
      """.stripMargin
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col2 as col3
        |FROM db2.view
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    modelFactory.analyzeView(viewQuery)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col3")
    assert(tableDeps.head.columns.head.value === new ColumnDependency("col1", "db2", "source"))
  }

  "a query with an aliased column in two views should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val viewQuery1 = "CREATE VIEW db2.view1 AS SELECT col1 as col2 FROM db2.source"
    val viewQuery2 = "CREATE VIEW db2.view2 AS SELECT col2 as col3 FROM db2.view1"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col3 as col4
        |FROM db2.view2
        |""".stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(flamyContext, tableGraph)
    modelFactory.analyzeCreate(createQuery)
    modelFactory.analyzeView(viewQuery1)
    modelFactory.analyzeView(viewQuery2)
    val tableDeps: Seq[TableDependency] = modelFactory.analyzePopulateDependencies(populateQuery, isView = false, new Variables())
    assert(tableDeps.head.columns.head.columnName === "col4")
    assert(tableDeps.head.columns.head.value === new ColumnDependency("col1", "db2", "source"))
  }

  "a simple query on a partitioned table should succeed" in {
    val createQuery = "CREATE TABLE db1.dest (col1 INT) PARTITIONED BY (part1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest PARTITION(part1) SELECT col1, part1 FROM db2.source"
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1, part1], partitions[part1], tableDeps[db2.source], colDeps[col1, part1])"
    testSuccess(
      creates = createQuery :: Nil,
      populates = populateQuery :: Nil,
      expected = expected
    )
  }

  "a simple query with missing table should succeed" in {
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.source"
    val expected = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[col1])"
    testSuccess(
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "an undefined column with no missing table should fail" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT t1.WRONG_COLUMN FROM db2.Source T1"
    testFailure(createQuery::Nil, Nil, populateQuery::Nil)
  }

  "a wrong table reference should fail" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT, E STRUCT<col:INT>)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT MISSING.col1 FROM db2.source T1 "
    testFailure(createQuery::Nil, Nil, populateQuery::Nil)
  }

  "subquery" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT t1.col1 FROM (SELECT col1 FROM db2.Source) T1"
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[db2.source.col1])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "column defined from subquery" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT t1.c FROM (SELECT source.col1 as c FROM db2.Source) T1"
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[c=db2.source.col1], tableDeps[db2.source], colDeps[db2.source.col1])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "undefined column in subquery" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT t1.col1 FROM (SELECT c as col1 FROM db2.Source) T1"
    testFailure(createQuery::Nil, Nil, populateQuery::Nil)
  }

  "a missing table should be OK" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """FROM
        |(
        | SELECT *
        | FROM db2.source T1
        | JOIN db2.MISSING_TABLE T2
        | on T1.col1=T2.col1
        |) T
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col2 """.stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col2], tableDeps[db2.missing_table, db2.source], colDeps[*, col2, db2.missing_table.col1, db2.source.col1])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }


  "An unknown table reference in a WHERE clause with all table definitions should fail" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.source T1 WHERE E.col IS NOT NULL"
    testFailure(createQuery::Nil, Nil, populateQuery::Nil)
  }

  "An unknown table reference in a WHERE clause with some missing table definitions known should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.missing T1 WHERE E.col IS NOT NULL"
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.missing], colDeps[col1, E.col])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "A struct col in a WHERE clause should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT, E STRUCT<col:INT>)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.source T1 WHERE E.col IS NOT NULL"
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[db2.source.col1, db2.source.E.col])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "A MAP REDUCE query should succeed" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | REDUCE * using ''
        | AS colA, colB, colC
        | FROM (SELECT * FROM db2.source) mapped ;""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[colA, colB, colC], tableDeps[db2.source], colDeps[db2.source.*])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a query with a lateral view should succeed" in {
    val createQuery = "CREATE TABLE db2.source (label_map MAP<STRING,STRING>)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT label_map, label_key, label_value
        | FROM db2.source
        | LATERAL VIEW explode(label_map) T1 as label_key, label_value""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[label_map, label_key, label_value], tableDeps[db2.source], colDeps[db2.source.label_map])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a query with a lateral view and a missing column should fail" in {
    val createQuery = "CREATE TABLE db2.source (label_map MAP<STRING,STRING>)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT label_map, label_key, WRONG_COLUMN
        | FROM db2.source
        | LATERAL VIEW explode(label_map) T1 as label_key, label_value""".stripMargin
    testFailure(createQuery::Nil, Nil, populateQuery::Nil)
  }

  "a query with a lateral view and a select * should succeed" in {
    val createQuery = "CREATE TABLE db2.source (label_map MAP<STRING,STRING>)"
    val populateQuery =
      """FROM
        |(
        | SELECT *
        | FROM db2.source
        | LATERAL VIEW explode(label_map) T AS label_key, label_value
        |) T2
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT label_map, label_key, label_value""".stripMargin
    val expected = "TableDependency(type=REF, name=dest, schema=db1, columns[label_map, label_key, label_value], tableDeps[db2.source], colDeps[db2.source.label_map])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a create VIEW with multiple columns having the same name should fail" in {
    val createQuery = "CREATE TABLE db2.source (id STRING, number INT)"
    val viewQuery =  "CREATE VIEW db2.view AS SELECT id, *, T1.*, T2.* FROM db2.source T1 JOIN db2.source T2 "
    testFailure(createQuery::Nil, Nil, viewQuery::Nil)
  }

//  "a multiple CREATE VIEW should succeed" in {
//    val createQuery = "CREATE TABLE db2.source (id STRING, number INT)"
//    val viewQuery1: String = "CREATE VIEW db2.view1 AS SELECT id, number WHERE  "
//    val viewQuery: String  = "CREATE VIEW db2.view AS SELECT * FROM db2.source T1 JOIN db2.source T2 "
//    testFailure(createQuery::Nil, viewQuery::Nil)
//  }

  "a query with UNION ALL should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM
        |(
        |  SELECT col1, col2 FROM db2.source1
        |  UNION ALL
        |  SELECT col1, NULL as col2 FROM db2.source2
        |) T""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.col1, db2.source1.col2, db2.source2.col1])"
    testSuccess(
      creates = createQuery1::createQuery2::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a query with UNION ALL and stars should be OK bis" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col1 INT, col2 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT * FROM
        |(
        |  SELECT * FROM db2.source1
        |  UNION ALL
        |  SELECT * FROM db2.source2
        |) T
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1, col2], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.*, db2.source2.*])"
    testSuccess(
      creates = createQuery1::createQuery2::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with WHERE ... IN (SUB_QUERY) clauses should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col2 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM db2.source1
        |WHERE col1 IN (SELECT col2 FROM db2.source2)
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.col1, db2.source2.col2])"
    testSuccess(
      creates = createQuery1::createQuery2::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with WHERE ... IN (SUB_QUERY with external column ref) should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col2 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM db2.source1 T
        |WHERE T.col1 IN (SELECT col2 FROM db2.source2 WHERE col2 = T.col1)
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.col1, db2.source2.col2])"
    testSuccess(
      creates = createQuery1::createQuery2::Nil,
      populates = populateQuery::Nil,
      expected = None
    )
  }

  "a correct query with WHERE ... IN (SUB_QUERY) clauses should be OK bis" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM db2.source1 A
        |WHERE A.col1 IN (SELECT col1 FROM db2.source2)
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.col1, db2.source2.col1])"
    testSuccess(
      creates = createQuery1::createQuery2::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "an incorrect query with WHERE ... IN (SUB_QUERY) clauses should NOT be OK" ignore {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM db2.source1
        |WHERE col1 IN (SELECT col1 FROM db2.source2)
        |""".stripMargin
    testFailure(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)
  }

  "a correct query with WHERE EXISTS (SUB_QUERY) clauses should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col2 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM db2.source1
        |WHERE EXISTS (SELECT col2 FROM db2.source2)
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.col1, db2.source2.col2])"
    testSuccess(
      creates = createQuery1::createQuery2::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "an incorrect query with WHERE EXISTS (SUB_QUERY) clauses should NOT be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col2 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM db2.source1
        |WHERE EXISTS (SELECT col1 FROM db2.source2)
        |""".stripMargin
    testFailure(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)
  }

  "a correct query with a CTE and a LATERAL VIEW should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """WITH T AS (SELECT col1 FROM db2.source1)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col2
        |FROM T
        |LATERAL VIEW explode(col1) LV as col2
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col2], tableDeps[db2.source1], colDeps[db2.source1.col1])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with the same alias used in a CTE and a subquery should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """WITH T AS (SELECT col1 FROM db2.source1)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM (SELECT col1 FROM T) T
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1], colDeps[db2.source1.col1])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with a CTE and an unknown table with a complete name should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """WITH T AS (SELECT col1 FROM db2.source1)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM (SELECT T.col1 FROM db2.unknown_table) T
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.unknown_table], colDeps[T.col1])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with a CTE and an unknown table with a short name should not be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """WITH T AS (SELECT col1 FROM db2.source1)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM (SELECT T.col1 FROM T2) T
        |""".stripMargin
    testFailure(createQuery1::Nil, Nil, populateQuery::Nil)
  }

  "a correct query with the same alias used in a CTE and nested subqueries should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """WITH T AS (SELECT col1 FROM db2.source1 T)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1
        |FROM (
        |   SELECT col1 FROM T
        |   UNION ALL
        |   SELECT col1 FROM T
        |) T2
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1], colDeps[db2.source1.col1])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with the same alias used in a table and a subquery should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT S.col1
        |FROM db2.source1 S
        |JOIN (SELECT S.col1 FROM db2.source1 S) T
        |ON S.col1 = T.col1
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1], colDeps[db2.source1.col1])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with a DISTRIBUTE BY should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col1 as col2
        | FROM db2.source1
        | DISTRIBUTE BY col2
        | """.stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col2=db2.source1.col1], tableDeps[db2.source1], colDeps[db2.source1.col1], postColDeps[col2])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "an incorrect query with a DISTRIBUTE BY should NOT be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """
        | INSERT OVERWRITE TABLE user_model.daily_users
        | SELECT
        |   col1 as col2
        | FROM db2.source1
        | DISTRIBUTE BY col1
        | """.stripMargin
    testFailure(createQuery1::Nil, Nil, populateQuery::Nil)
  }

  "a correct query with a DISTRIBUTE BY and a prefixed column should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT S1.col1
        | FROM db2.source1 S1
        | DISTRIBUTE BY S1.col1
        | """.stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col2], tableDeps[db2.source1], colDeps[db2.source1.col1], postColDeps[col2])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = None
    )
  }

  /* In this context, S1.col1 should not be recognized because it is not in the SELECT clause... */
  "an incorrect query with a DISTRIBUTE BY and a one-time-prefixed column should NOT be OK" ignore {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col1
        | FROM db2.source1 S1
        | DISTRIBUTE BY S1.col1
        | """.stripMargin
    testFailure(createQuery1::Nil, Nil, populateQuery::Nil)
  }

  "a correct query with a ORDER BY should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val populateQuery =
      """
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT
        |   col1 as col2
        | FROM db2.source1
        | ORDER BY col2
        | """.stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col2=db2.source1.col1], tableDeps[db2.source1], colDeps[db2.source1.col1], postColDeps[col2])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with partition variables should succeed" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 STRING, part3 INT, part4 STRING)"
    val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 STRING, part3 INT, part4 STRING)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1, part2, part3, part4)
        | SELECT
        | col1,
        | col2,
        | ${partition:part1} as part1,
        | ${partition:part2} as part2,
        | 0 as part3,
        | "0" as part4
        | FROM db2.source1 S1
        |""".stripMargin
    val expected = "TableDependency(type=REF, name=dest, schema=db1, columns[col1, col2], partitions[part1, part2, part3=0, part4=0], tableDeps[db2.source1], colDeps[db2.source1.col1, db2.source1.col2])"
    testSuccess(
      creates = createQuery1 :: createQuery2 :: Nil,
      populates = populateQuery :: Nil,
      variables = new Variables("partition:part1" -> "${partition:part1}", "partition:part2" -> "${partition:part2}"),
      expected = Option(expected)
    )
  }


  "column name checking: " - {
    "a table with correct partition specification should be ok" in {
      val createQuery = "CREATE TABLE db1.dest (col1 INT) PARTITIONED BY (partA STRING)"
      val populateQuery = "INSERT OVERWRITE TABLE db1.dest PARTITION(partA) SELECT col1, 'A' as partA FROM db2.source"
      testSuccess(createQuery::Nil, Nil, populateQuery::Nil)
    }

    "a table with partition specification that differs in the CREATE and the POPULATE should fail" in {
      val createQuery = "CREATE TABLE db1.dest (col1 INT) PARTITIONED BY (partA STRING)"
      val populateQuery = "INSERT OVERWRITE TABLE db1.dest PARTITION(wrongPartition) SELECT col1, 'A' as wrongPartition FROM db2.source"
      testFailure(createQuery::Nil, Nil, populateQuery::Nil)
    }

    "a missing column from a VIEW should fail" in {
      val createQuery = "CREATE TABLE db2.source (col1 INT)"
      val viewQuery =  "CREATE VIEW db2.view AS SELECT col1 as col2 FROM db2.source"
      val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.view"
      testFailure(createQuery::Nil, viewQuery::Nil, populateQuery::Nil)
    }

    "a query with JOIN with tables with same column names should be OK" in {
      val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
      val createQuery2 = "CREATE TABLE db2.source2 (col1 INT)"
      val populateQuery =
        """INSERT OVERWRITE TABLE db1.dest
          |SELECT T1.col1 FROM
          |(
          |  SELECT col1 FROM db2.source1
          |) T1
          |JOIN
          |(
          |  SELECT col1 FROM db2.source1
          |) T2
          |ON T1.col1 = T2.col1
          |""".stripMargin
      val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1], colDeps[db2.source1.col1])"
      testSuccess(
        creates = createQuery1::createQuery2::Nil,
        populates = populateQuery::Nil,
        expected = expected
      )
    }

    "a column existing in a table used in a subquery but not kept in that subquery should fail" in {
      val createQuery = "CREATE TABLE db2.source (label_map MAP<STRING,STRING>)"
      val populateQuery =
        """FROM
          |(
          | SELECT 1 as label
          | FROM db2.source
          |) T
          |INSERT OVERWRITE TABLE db1.dest
          |SELECT label_map""".stripMargin
      testFailure(createQuery::Nil, Nil, populateQuery::Nil)
    }

    "a column used in a WHERE clause in a subquery but not kept should fail" in {
      val createQuery = "CREATE TABLE db2.source (col1 INT)"
      val populateQuery =
        """FROM (
          | SELECT 1 as col2
          | FROM db2.source
          | WHERE col1 >= 1
          |) T
          |INSERT OVERWRITE TABLE db1.dest
          |SELECT col1""".stripMargin
      testFailure(createQuery::Nil, Nil, populateQuery::Nil)
    }

    "a column used in a WHERE clause in a CTE but not kept should fail" in {
      val createQuery = "CREATE TABLE db2.source (col1 INT)"
      val populateQuery =
        """WITH T AS (
          | SELECT 1 as col2
          | FROM db2.source
          | WHERE col1 >= 1
          |)
          |INSERT OVERWRITE TABLE db1.dest
          |SELECT col1 FROM T""".stripMargin
      testFailure(createQuery::Nil, Nil, populateQuery::Nil)
    }

    "a * column FROM two sub-query should be correct" in {
      val populateQuery =
        """INSERT OVERWRITE TABLE db1.dest
          |SELECT *
          |FROM (SELECT 1 as col1) T1
          |JOIN (SELECT 2 as col2) T2
          |""".stripMargin
      val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1=1, col2=2])"
      testSuccess(
        populates = populateQuery::Nil,
        expected = expected
      )
    }

    "an ambiguous column available in two known tables should fail" in {
      val createQuery1 = "CREATE TABLE db2.source1 (id STRING)"
      val createQuery2 = "CREATE TABLE db2.source2 (id STRING)"
      val populateQuery =
          """
            | INSERT OVERWRITE TABLE user_model.daily_users
            | SELECT
            |   id
            | FROM db2.source1 S1
            | JOIN db2.source2 S2
            | ON S1.id = S2.id
            | """.stripMargin
      testFailure(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)
    }

    "an ambiguous column available in a known tables and a subquery should fail" in {
      val createQuery1 = "CREATE TABLE db2.source1 (id STRING)"
      val createQuery2 = "CREATE TABLE db2.source2 (id STRING)"
      val populateQuery =
          """
            | INSERT OVERWRITE TABLE user_model.daily_users
            | SELECT
            |   id
            | FROM db2.source1 S1
            | JOIN (SELECT id FROM db2.source2) S2
            | ON S1.id = S2.id
            | """.stripMargin
      testFailure(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)
    }

    "an ambiguous column available in a known table and a LATERAL VIEW should fail" in {
      val createQuery = "CREATE TABLE db2.source (id STRING, number INT)"
      val populateQuery =
          """
            | INSERT OVERWRITE TABLE user_model.daily_users
            | SELECT
            |   number, id
            | FROM db2.source S
            | LATERAL VIEW OUTER explode(array(1)) LV as number
            | """.stripMargin
      val expected: String = "TableDependency(type=REF, name=daily_users, schema=user_model, columns[id, number2], tableDeps[db2.source], colDeps[db2.source.id, db2.source.number], bothColDeps[db2.source.number])"
      testFailure(createQuery::Nil, Nil, populateQuery::Nil)
    }

    "a correct query with a DISTRIBUTE BY and a one-time-prefixed column should be OK" in {
      val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
      val populateQuery =
        """
          | INSERT OVERWRITE TABLE db1.dest
          | SELECT S1.col1
          | FROM db2.source1 S1
          | DISTRIBUTE BY col1
          | """.stripMargin
      val expected = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1], colDeps[db2.source1.col1], postColDeps[col1])"
      testSuccess(
        creates = createQuery1::Nil,
        populates = populateQuery::Nil,
        expected = expected
      )
    }

    "an ambiguous column in a DISTRIBUTE BY or SORT BY clause should be OK" in {
      val createQuery1 = "CREATE TABLE db2.source1 (col1 STRING)"
      val createQuery2 = "CREATE TABLE db2.source2 (col1 STRING)"
      val populateQuery =
          """ INSERT OVERWRITE TABLE user_model.daily_users
            | SELECT
            |   S1.col1
            | FROM db2.source1 S1
            | JOIN db2.source2 S2
            | ON S1.col1 = S2.col1
            | DISTRIBUTE BY col1
            | SORT BY col1
            | """.stripMargin
//      val expected = "TableDependency(type=REF, name=daily_users, schema=user_model, columns[id], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.id, db2.source2.id], postColDeps[id])"
      testSuccess(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)
    }

    "an ambiguous column in a ORDER BY clause and present in the SELECT should be OK" in {
      val createQuery1 = "CREATE TABLE db2.source1 (id STRING)"
      val createQuery2 = "CREATE TABLE db2.source2 (id STRING)"
      val populateQuery =
          """ INSERT OVERWRITE TABLE user_model.daily_users
            | SELECT
            |   S1.id
            | FROM db2.source1 S1
            | JOIN db2.source2 S2
            | ON S1.id = S2.id
            | ORDER BY id
            | """.stripMargin
      val expected = "TableDependency(type=REF, name=daily_users, schema=user_model, columns[id], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.id, db2.source2.id], postColDeps[id])"
      testSuccess(
        creates = createQuery1::createQuery2::Nil,
        populates = populateQuery::Nil,
        expected = expected
      )
    }

    "an column in a ORDER BY but absent from the SELECT should fail" in {
      val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
      val populateQuery =
        """
          | INSERT OVERWRITE TABLE user_model.daily_users
          | SELECT
          |   col1 as col2
          | FROM db2.source1
          | ORDER BY col1
          | """.stripMargin
      testFailure(createQuery1::Nil, Nil, populateQuery::Nil)
    }

    "an ambiguous column in a ORDER BY clause and absent from the SELECT should fail" in {
      val createQuery1 = "CREATE TABLE db2.source1 (id STRING)"
      val createQuery2 = "CREATE TABLE db2.source2 (id STRING)"
      val populateQuery =
          """ INSERT OVERWRITE TABLE user_model.daily_users
            | SELECT
            |   S1.id as toto
            | FROM db2.source1 S1
            | JOIN db2.source2 S2
            | ON S1.id = S2.id
            | ORDER BY id
            | """.stripMargin
      val expected = "TableDependency(type=REF, name=daily_users, schema=user_model, columns[id], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.id, db2.source2.id], postColDeps[id])"
      testFailure(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)
    }


  }

  "column number checking : a table" - {
    "with an incorrect number of columns should fail" in {
      val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
      val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT)"
      val populateQuery =
        """ INSERT OVERWRITE TABLE db1.dest
        | SELECT col1
        | FROM db2.source1 S1
        |""".stripMargin
      testFailure(createQuery1:: createQuery2::Nil, Nil, populateQuery::Nil)
    }
  }

  "partition number checking: " - {
    "a table with dynamic partitioning" - {

      "with an correct number of partition in the select should succeed" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1, part2)
            | SELECT
            | col1,
            | col2,
            | "1" as part1,
            | "2" as part2
            | FROM db2.source1 S1
            |""".stripMargin
        val expected = "TableDependency(type=REF, name=dest, schema=db1, columns[col1, col2], partitions[part1=1, part2=2], tableDeps[db2.source1], colDeps[db2.source1.col1, db2.source1.col2])"
        testSuccess(
          creates = createQuery1::createQuery2 :: Nil,
          populates = populateQuery::Nil,
          expected = expected
        )
      }

      "with an incorrect number of partition in the select should fail" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1, part2)
            | SELECT col1, col2, col1 as part1
            | FROM db2.source1 S1
            |""".stripMargin
        testFailure(createQuery1::createQuery2 :: Nil, Nil, populateQuery::Nil)
      }

      "with an incorrect number of partition and a SELECT * should fail" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery = """" INSERT OVERWRITE TABLE db1.dest PARTITION(part1, part2)
                                      | SELECT *
                                      | FROM db2.source1 S1
                                      |""".stripMargin
        testFailure(createQuery1:: createQuery2::Nil, Nil, populateQuery::Nil)
      }

      "with an incorrect number of declared partition should fail" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1)
            | SELECT col1, col2, col1 as part1, col2 as part2
            | FROM db2.source1 S1
            |""".stripMargin
        testFailure(createQuery1::createQuery2:: Nil, Nil, populateQuery ::Nil)
      }

      "with an incorrect number of partition everywhere should fail" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1)
            | SELECT col1, col2, col1 as part1
            | FROM db2.source1 S1
            |""".stripMargin
        testFailure(createQuery1::createQuery2:: Nil, Nil, populateQuery ::Nil)
      }
    }

    "a table with no dynamic partitioning" - {

      "with an correct number of partitions in the select should succeed" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1=1, part2=2)
            | SELECT col1, col2
            | FROM db2.source1 S1
            |""".stripMargin
        val expected = "TableDependency(type=REF, name=dest, schema=db1, columns[col1, col2], partitions[part1=1, part2=2], tableDeps[db2.source1], colDeps[db2.source1.col1, db2.source1.col2])"
        testSuccess(
          creates = createQuery1::createQuery2 :: Nil,
          populates = populateQuery::Nil,
          expected = expected
        )
      }

     "with an incorrect number of partitions in the select should fail" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1=1, part2=2)
            | SELECT col1, col2, col1 as part1
            | FROM db2.source1 S1
            |""".stripMargin
        testFailure(createQuery1::createQuery2 :: Nil, Nil, populateQuery::Nil)
      }

      "with an incorrect number of declared partitions should fail" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1=1)
            | SELECT col1, col2
            | FROM db2.source1 S1
            |""".stripMargin
        testFailure(createQuery1::createQuery2:: Nil, Nil, populateQuery ::Nil)
      }

      "with an incorrect number of partitions everywhere should fail" in {
        val createQuery1 = "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery =
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1=1)
            | SELECT col1, col2, col2 as part2
            | FROM db2.source1 S1
            |""".stripMargin
        testFailure(createQuery1::createQuery2:: Nil, Nil, populateQuery ::Nil)
      }
    }
  }

  "a correct CREATE VIEW query with WHERE ... IN should be OK" in {
    val populateQuery =
      """CREATE VIEW db1.view
        |AS
        |SELECT * FROM (SELECT 1 as client_id) A
        |WHERE A.client_id NOT IN (SELECT 1 as client_id)
        |""".stripMargin
    testSuccess(
      populates = populateQuery::Nil,
      expected = None
    )
  }

  "a correct query with subquery and distribute by should be OK" in {
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT T0.*
        | FROM (
        |    SELECT D.day
        |    FROM db2.source D
        |    ) T0
        | DISTRIBUTE BY day -- use this when generating many partitions
        |""".stripMargin
    testSuccess(
      populates = populateQuery::Nil,
      expected = None
    )
  }

  "a correct query with LATERAL VIEW on a subquery should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (arr ARRAY<INT>)"
    val populateQuery =
      """
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT a
        |  FROM (
        |    SELECT arr FROM db2.source1
        |  ) T
        |LATERAL VIEW explode(T.arr) LV as a
        |""".stripMargin
    testSuccess(createQuery1::Nil, Nil, populateQuery::Nil)
  }

  "Creating a view with wrong column names should fail" in {
    val createQuery = "CREATE TABLE db2.source (id STRING, number INT)"
    val viewQuery = "CREATE VIEW db2.view AS SELECT id, *, T1.*, T2.* FROM db2.source T1 JOIN db2.source T2 "
    testFailure(createQuery::Nil, viewQuery::Nil, Nil)
  }

  "a correct query with HAVING should succeed" in {
    val createQuery = "CREATE TABLE db2.source (id STRING, number INT)"
    val populateQuery =
        """
          | INSERT OVERWRITE TABLE user_model.daily_users
          | SELECT
          |   id,
          |   COUNT(1) as nb
          | FROM db2.source
          | GROUP BY id
          | HAVING nb > 10
          | """.stripMargin
    val expected: String = "TableDependency(type=REF, name=daily_users, schema=user_model, columns[id, nb], tableDeps[db2.source], colDeps[db2.source.id], bothColDeps[nb])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with HAVING on aggregation should succeed" in {
    val createQuery = "CREATE TABLE db2.source (id STRING, number INT)"
    val populateQuery =
      """
        | INSERT OVERWRITE TABLE user_model.daily_users
        | SELECT
        |   id,
        |   COUNT(1) as nb
        | FROM db2.source
        | GROUP BY id
        | HAVING SUM(number) > 10
        | """.stripMargin
    val expected: String = "TableDependency(type=REF, name=daily_users, schema=user_model, columns[id, nb], tableDeps[db2.source], colDeps[db2.source.id], bothColDeps[db2.source.number])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with HAVING on aggregation with same name should succeed" in {
    val createQuery = "CREATE TABLE db2.source (id INT, number INT)"
    val populateQuery =

        """INSERT OVERWRITE TABLE db1.dest
          |SELECT
          |  T.id
          |FROM ( SELECT * FROM db2.source ) T
          |GROUP BY id
          |HAVING T.number = 1
          |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[id], tableDeps[db2.source], colDeps[db2.source.*, db2.source.id])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "wrong column name in HAVING" in {
    val createQuery = "CREATE TABLE db2.source (col1 INT)"
    val populateQuery = "INSERT OVERWRITE TABLE db1.dest SELECT COUNT(1) as num FROM db2.source HAVING unknown_column > 0"
    testFailure(createQuery::Nil, Nil, populateQuery::Nil)
  }

  "a query with a lateral view should not be bugged" in {
    val createQuery = "CREATE TABLE db2.source (label_map MAP<STRING,STRING>)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT label_map, label_key, label_value, TOK_FUNCTION, TOK_TABALIAS
        | FROM db2.source
        | LATERAL VIEW explode(label_map) T1 as label_key, label_value""".stripMargin
    //    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[label_map, label_key, label_value], tableDeps[db2.source], colDeps[db2.source.label_map])"
    testFailure(createQuery::Nil, Nil, populateQuery::Nil)
  }

  "a correct query with LEFT SEMI JOIN should be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col1 INT, col2 INT)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT col1
        | FROM db2.source1 S1
        | LEFT SEMI JOIN db2.source2 S2
        | ON S1.col1 = S2.col1
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1, db2.source2], colDeps[db2.source1.col1, db2.source2.col1])"
    testSuccess(
      creates = createQuery1::createQuery2::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "an incorrect query with LEFT SEMI JOIN should NOT be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col1 INT, col2 INT)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT col2
        | FROM db2.source1 S1
        | LEFT SEMI JOIN db2.source2 S2
        | ON S1.col1 = S2.col1
        |""".stripMargin
    testFailure(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)
  }

  "an incorrect query with LEFT SEMI JOIN and same alias used twice should NOT be OK" in {
    val createQuery1 = "CREATE TABLE db2.source1 (col1 INT)"
    val createQuery2 = "CREATE TABLE db2.source2 (col1 INT, col2 INT)"
    val populateQuery =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT col1
        | FROM db2.source1 S1
        | LEFT SEMI JOIN db2.source2 S1
        | ON S1.col1 = S2.col1
        |""".stripMargin
    testFailure(createQuery1::createQuery2::Nil, Nil, populateQuery::Nil)

  }

  "a query with joins should be correctly parsed" in {
    val query =
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
    val expected: String = "TableDependency(type=REF, name=toto, schema=db1, columns[colA], tableDeps[db10.tutu, db11.tutu, db2.tutu, db3.tutu, db4.tutu, db5.tutu, db6.tutu, db7.tutu, db8.tutu, db9.tutu], colDeps[colA, db10.tutu.id, db11.tutu.id, db2.tutu.id, db3.tutu.id, db4.tutu.id, db5.tutu.id, db6.tutu.id, db7.tutu.id, db8.tutu.id, db9.tutu.id])"
    testSuccess(
      populates = query :: Nil,
      expected = expected
    )
  }

  "a correct query with a multi-insert should succeed" in {
    val createQuery1 = "CREATE TABLE db2.source (col1 INT, col2 INT, col3 INT, col4 INT)"
    val populateQuery =
      """FROM (SELECT col1, col2, col3, col4 FROM db2.source) T
        |INSERT OVERWRITE TABLE db1.dest SELECT col1 WHERE col2 > 0
        |INSERT OVERWRITE TABLE db1.dest SELECT col3 WHERE col4 < 0
        |""".stripMargin
    val expected: String = "TableDependency(type=REF, name=dest, schema=db1, columns[col3], tableDeps[db2.source], colDeps[db2.source.col1, db2.source.col2, db2.source.col3, db2.source.col4])"
    testSuccess(
      creates = createQuery1::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }

  "a correct query with a table alias in a lateral view should succeed" in {
    val createQuery = "CREATE TABLE db2.source (id STRING, number INT)"
    val populateQuery =
        """
          | INSERT OVERWRITE TABLE user_model.daily_users
          | SELECT
          |   S.id, LV.number
          | FROM db2.source S
          | LATERAL VIEW OUTER explode(array(1)) LV as number
          | """.stripMargin
    val expected: String = "TableDependency(type=REF, name=daily_users, schema=user_model, columns[id, number], tableDeps[db2.source], colDeps[db2.source.id])"
    testSuccess(
      creates = createQuery::Nil,
      populates = populateQuery::Nil,
      expected = expected
    )
  }


}
