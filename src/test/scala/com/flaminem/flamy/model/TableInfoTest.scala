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

package com.flaminem.flamy.model

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.graph.TableGraph
import com.flaminem.flamy.model.core.CompleteModelFactory
import com.flaminem.flamy.model.files.TableFile
import org.scalatest.FreeSpec

import scala.language.implicitConversions

/**
  * Created by fpin on 7/14/16.
  */
class TableInfoTest extends FreeSpec {

  implicit def stringPairToTableFile(pair: (String, String)): TableFile = {
    new DummyTableFile(pair._1, pair._2)
  }

  val context = new FlamyContext("flamy.model.dir.paths" -> "src/empty_test/resources/meta-dir")
  val tableGraph = TableGraph(context, Nil, checkNoMissingTable = false)

  def testSuccess(creates: Seq[(String, String)], populates: Seq[(String, String)], expected: String): Unit = {
    testSuccess(creates, populates, Option(expected))
  }

  def test(creates: Seq[(String, String)], populates: Seq[(String, String)], variables: Variables = new Variables()): TableInfo = {
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(context, tableGraph)
    creates.foreach{modelFactory.analyzeCreate(_)}
    populates.foreach{modelFactory.analyzePopulate(_, variables)}
    modelFactory.mergeableTableInfoSet.head
  }

  def testSuccess(creates: Seq[(String, String)], populates: Seq[(String, String)], expected: Option[String] = None): Unit = {
    val td: TableInfo = test(creates, populates)
    expected match {
      case Some(e) => assert(e === td.toString)
      case None => System.out.println(td.toString)
    }
  }

  "a query with a multi-insert should be OK" in {
    val createQuery1 = "db1.source" -> "CREATE TABLE db1.source (col1 INT) PARTITIONED BY (part1 STRING)"
    val createQuery2 = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT) PARTITIONED BY (part1 STRING, part2 STRING)"
    val populateQuery =
      "db1.dest" ->
       """ FROM (SELECT col1 FROM db1.source WHERE part1 = ${partition:part1}) T
         | INSERT OVERWRITE TABLE db1.dest PARTITION(part1 = ${partition:part1}, part2 = "A")
         | SELECT col1 WHERE col1 = "A"
         | INSERT OVERWRITE TABLE db1.dest PARTITION(part1 = ${partition:part1}, part2 = "B")
         | SELECT col1 WHERE col1 = "B"
       """.stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(context, tableGraph)
    val creates = createQuery1::createQuery2::Nil
    val populates = populateQuery::Nil
    creates.foreach{modelFactory.analyzeCreate(_)}
    val variables = new Variables("partition:part1" -> "${partition:part1}")
    populates.map{case (tableName, text) => new DummyTableFile(tableName, text)}.foreach{modelFactory.analyzePopulate(_, variables)}
    val td: TableInfo = modelFactory.mergeableTableInfoSet.head
    assert(td.populateInfos.size === 1)
    assert(td.populateInfos.head.constantPartitions.size == 2)
  }

  "a query with a constant partition should be OK" in {
    val createQuery1 = "db1.source" -> "CREATE TABLE db1.source (col1 INT) PARTITIONED BY (part1 STRING)"
    val createQuery2 = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT) PARTITIONED BY (part1 STRING, part2 STRING)"
    val populateQuery =
      "db1.dest" ->
         """INSERT OVERWRITE TABLE db1.dest PARTITION(part1, part2)
           |SELECT
           |  col1,
           |  ${partition:part1} as part1,
           |  1 as part2
           |FROM db1.source
           |WHERE part1 = ${partition:part1}
       """.stripMargin
    val modelFactory: CompleteModelFactory = new CompleteModelFactory(context, tableGraph)
    val creates = createQuery1::createQuery2::Nil
    val populates = populateQuery::Nil
    creates.foreach{modelFactory.analyzeCreate(_)}
    val variables = new Variables("partition:part1" -> "${partition:part1}")
    populates.map{case (tableName, text) => new DummyTableFile(tableName, text)}.foreach{modelFactory.analyzePopulate(_, variables)}
    val td: TableInfo = modelFactory.mergeableTableInfoSet.head
    assert(td.populateInfos.size === 1)
    assert(td.populateInfos.head.constantPartitions.size == 1)
  }

  "SELECT * FROM a missing table should be OK" in {
    val createQuery = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT)"
    val populateQuery = "db1.dest" -> "INSERT OVERWRITE TABLE db1.dest SELECT day FROM (SELECT A.* FROM db2.UNKNOWN_TABLE A) L"
    val expected: String = "TableInfo(type=TABLE, name=db1.dest, columns[Column(name=col1, type=int)], tableDeps[db2.unknown_table])"
    testSuccess(createQuery::Nil, populateQuery::Nil, expected)
  }

  "a text with partition transformation should be OK" ignore {
    val createQuery1 = "db1.source" -> "CREATE TABLE db1.source (id STRING) PARTITIONED BY (part1 STRING)"
    val createQuery2 = "db2.dest" -> "CREATE TABLE db2.dest (id INT) PARTITIONED BY (part2 STRING)"
    val populateQuery = "db2.dest" ->
      """
        |@regen(
        | IGNORE TIMESTAMP db1.source ;
        | IGNORE TIMESTAMP db1.source ;
        |)
        |
        |INSERT OVERWRITE TABLE db2.dest PARTITION(part2)
        |SELECT id, part1
        |FROM db1.source
        |;
      """.stripMargin
    val td: TableInfo = test(createQuery1::createQuery2::Nil, populateQuery::Nil)
    td.populateInfos.map{_.annotations}.foreach{println}
//
//    //    val expected: String = "TableInfo(type=TABLE, name=db1.dest, columns[Column(name=col1, type=int)], tableDeps[db2.unknown_table])"
//    testSuccess(createQuery1::createQuery2::Nil, populateQuery::Nil, None)
  }

  "SELECT T.col FROM a missing table should be OK" in {
    val createQuery = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT)"
    val populateQuery = "db1.dest" -> "INSERT OVERWRITE TABLE db1.dest SELECT L.day FROM (SELECT * FROM db2.UNKNOWN_TABLE) L"
    val expected: String = "TableInfo(type=TABLE, name=db1.dest, columns[Column(name=col1, type=int)], tableDeps[db2.unknown_table])"
    testSuccess(createQuery::Nil, populateQuery::Nil, expected)
  }

  "a query reading from a VIEW should succeed" in {
    val createQuery = "db2.source" -> "CREATE TABLE db2.source (id STRING, number INT)"
    val viewQuery = "db2.view" -> "CREATE VIEW db2.view AS SELECT id as id1, * FROM db2.source"
    val populateQuery = "db1.dest" -> "INSERT OVERWRITE TABLE db1.dest SELECT id, number FROM db2.view"
    val expected: String = "TableInfo(type=REF, name=db1.dest, columns[id, number], tableDeps[db2.view])"
    testSuccess(createQuery::Nil, viewQuery::populateQuery::Nil, expected)
  }

  "column number checking : a table" - {
    "with the correct number of columns should succeed" in {
      val createQuery1 = "db2.source1" -> "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
      val createQuery2 = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT, col2 INT)"
      val populateQuery = "db1.dest" ->
        """ INSERT OVERWRITE TABLE db1.dest
          | SELECT col1, col2
          | FROM db2.source1 S1
          |""".
          stripMargin
      val expected = "TableInfo(type=TABLE, name=db1.dest, columns[Column(name=col1, type=int), Column(name=col2, type=int)], tableDeps[db2.source1])"
      testSuccess(createQuery1 :: createQuery2 :: Nil, populateQuery :: Nil, expected)
    }
  }

  "partition checking: " - {
    "a table with dynamic partitioning" - {

      "with the correct number of partition should succeed" in {
        val createQuery1 = "db2.source1" -> "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery = "db1.dest" ->
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1, part2)
            | SELECT col1, col2, col1 as part1, col2 as part2
            | FROM db2.source1 S1
            |""".stripMargin
        val expected = "TableInfo(type=TABLE, name=db1.dest, columns[Column(name=col1, type=int), Column(name=col2, type=int)], partitions[Column(name=part1, type=int), Column(name=part2, type=int)], tableDeps[db2.source1])"
        val tableInfo: TableInfo = test(createQuery1:: createQuery2::Nil, populateQuery::Nil)
        assert(tableInfo.toString === expected)
        assert(tableInfo.populateInfos.size === 1)
        assert(tableInfo.populateInfos.head.hasDynamicPartitions === true)
      }

      "with the correct number of partition and a SELECT * should succeed" in {
        val createQuery1 = "db1.source1" -> "CREATE TABLE db1.source1 (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val createQuery2 = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery = "db1.dest" -> "INSERT OVERWRITE TABLE db1.dest PARTITION(part1, part2) SELECT * FROM db1.source1"
        val expected = "TableInfo(type=TABLE, name=db1.dest, columns[Column(name=col1, type=int), Column(name=col2, type=int)], partitions[Column(name=part1, type=int), Column(name=part2, type=int)], tableDeps[db1.source1])"
        testSuccess(createQuery1:: createQuery2 ::Nil, populateQuery::Nil, expected)
      }

    }

    "a table with no dynamic partitioning" - {
      "with the correct number of partition should succeed" in {
        val createQuery1 = "db2.source1" -> "CREATE TABLE db2.source1 (col1 INT, col2 INT)"
        val createQuery2 = "db1.dest" -> "CREATE TABLE db1.dest (col1 INT, col2 INT) PARTITIONED BY (part1 INT, part2 INT)"
        val populateQuery = "db1.dest" ->
          """ INSERT OVERWRITE TABLE db1.dest PARTITION(part1=1, part2=2)
            | SELECT col1, col2
            | FROM db2.source1 S1
            |""".stripMargin
        val expected = "TableInfo(type=TABLE, name=db1.dest, columns[Column(name=col1, type=int), Column(name=col2, type=int)], partitions[Column(name=part1, type=int), Column(name=part2, type=int)], tableDeps[db2.source1])"
        val tableInfo: TableInfo = test(createQuery1:: createQuery2::Nil, populateQuery::Nil)
        assert(tableInfo.toString === expected)
        assert(tableInfo.populateInfos.size === 1)
        assert(tableInfo.populateInfos.head.hasDynamicPartitions === false)
      }
    }
  }
}
