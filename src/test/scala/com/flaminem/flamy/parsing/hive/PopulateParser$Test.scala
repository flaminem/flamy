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
import com.flaminem.flamy.model.exceptions.FlamyException
import com.flaminem.flamy.model.partitions.Partition
import com.flaminem.flamy.model.{PartitionColumn, Variables}
import org.scalatest.FreeSpec

/**
 * Created by fpin on 5/25/15.
 */
class PopulateParser$Test extends FreeSpec {

  implicit val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")

  def testSuccess(text: String, expected: String, isView: Boolean = false) {
    val res = PopulateParser.parseText(text, new Variables(), isView).mkString("[",", ","]")
    if (expected != null) {
      assert(res.toString === expected)
    }
    else {
      System.out.println(res.toString)
    }
  }

  def testFailure(query: String, isView: Boolean = false) {
    intercept[FlamyParsingException]{
      PopulateParser.parseQuery(query)
    }
  }

  "constant partition parsing : "  - {

    "should correctly parse constant partitions" in {
      val text: String =
        """INSERT OVERWRITE TABLE db1.dest
          |PARTITION (
          |  intPart = 0,
          |  stringPart = "string"
          |)
          |SELECT
          |id
          |FROM db2.source
          |""".stripMargin
      val actual =
        PopulateParser.parseText(text, new Variables(), isView = false)
        .map{td => new Partition(td.partitions.filter{_.hasConstantValue})}
      val expected = Seq(new Partition("intPart=0/stringPart=string"))
      assert(actual === expected)
    }

    "should correctly parse constant partitions with weird spacing" in {
      val text: String =
        """INSERT OVERWRITE TABLE db1.dest
          |PARTITION(intPart
          |  =
          |  0
          |  ,
          |  stringPart="string")
          |SELECT
          |id
          |FROM db2.source
          |""".stripMargin
      val actual =
        PopulateParser.parseText(text, new Variables(), isView = false)
        .map{td => new Partition(td.partitions.filter{_.hasConstantValue})}
      val expected = Seq(new Partition("intPart=0/stringPart=string"))
      assert(actual === expected)
    }

    "should return a singleton with an empty partition if there is no constant partition to be found" in {
      val text: String =
        """INSERT OVERWRITE TABLE db1.dest
          |SELECT
          |id
          |FROM db2.source
          |""".stripMargin
      val actual =
        PopulateParser.parseText(text, new Variables(), isView = false)
        .map{td => new Partition(td.partitions.filter{_.hasConstantValue})}
      val expected = Seq(new Partition(Nil))
      assert(actual === expected)
    }

    "should correctly parse constant partitions in a query with quotes" in {
      val text: String =
        """WITH T AS (
          | SELECT
          | "insert overwrite table trap.t1 partition(stringPart='0')" ,
          | 'insert overwrite table trap.t2 partition(string="0")' ,
          | 'insert overwrite table trap.t3 partition(string=\'0\')' ,
          | "insert overwrite table trap.t3 partition(string=\"0\")"
          |)
          |INSERT OVERWRITE TABLE db1.dest
          |PARTITION(
          |  intPart = 0,
          |  stringPart = "string"
          |)
          |SELECT
          |id
          |FROM db2.source
          |""".stripMargin
      val actual =
        PopulateParser.parseText(text, new Variables(), isView = false)
        .map{td => new Partition(td.partitions.filter{_.hasConstantValue})}
      val expected = Seq(new Partition("intPart=0/stringPart=string"))
      assert(actual === expected)
    }

    "should correctly parse constant partitions from multiple queries" in {
      val text: String =
        """INSERT OVERWRITE TABLE db1.dest
          |PARTITION(
          |  intPart = 0,
          |  stringPart = ${partition:stringPart}
          |)
          |SELECT
          |id
          |FROM db2.source
          |;
          |INSERT OVERWRITE TABLE db1.dest
          |PARTITION(
          |  intPart = ${partition:intPart},
          |  stringPart = "string"
          |)
          |SELECT
          |id
          |FROM db2.source
          |""".stripMargin
      val variables = new Variables("partition:stringPart" -> "${partition:stringPart}'0'", "partition:intPart" -> "${partition:intPart}0")
      val actual =
        PopulateParser.parseText(text, variables, isView = false)
        .map{td => new Partition(td.partitions.filter{_.hasConstantValue})}
      val expected = Seq(new Partition("intPart=0"), new Partition("stringPart=string"))
      assert(actual === expected)
    }

    "should correctly parse constant partitions from multiple queries with an empty query" in {
      val text: String =
        """SET hive.map.aggr = false ;
            |INSERT OVERWRITE TABLE db1.dest
            |PARTITION(
            |  intPart = 0,
            |  stringPart = ${partition:stringPart}
            |)
            |SELECT
            |id
            |FROM db2.source
            |;
            |""".stripMargin
      val variables = new Variables("partition:stringPart" -> "${partition:stringPart}'0'")
      val actual =
        PopulateParser.parseText(text, variables, isView = false)
          .map{td => new Partition(td.partitions.filter{_.hasConstantValue})}
      val expected = Seq(new Partition("intPart=0"))
      assert(actual === expected)
    }

    "should correctly parse constant partitions in queries with dynamic partitioning " in {
      val text =
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
      val variables = new Variables("partition:part1" -> "${partition:part1}0", "partition:part2" -> "${partition:part2}'0'")
      val actual =
        PopulateParser.parseText(text, variables, isView = false)
          .map{td => new Partition(td.columns.filter{_.hasConstantValue}.map{new PartitionColumn(_)})}
      val expected = Seq(new Partition("part3=0/part4=0"))
      assert(actual === expected)
    }
  }


  "it should correctly parse a LOAD DATA query" in {
    val query: String = "LOAD DATA INPATH '' OVERWRITE INTO TABLE db1.test PARTITION (day=\'\')"
    val expected: String = "[TableDependency(type=REF, name=test, schema=db1, partitions[day=])]"
    testSuccess(query, expected)
  }

  "it should correctly parse a SET query" in {
    val query: String = "SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec"
    testSuccess(query, "[]")
  }

  "it should correctly parse simple queries 1" in {
    val query = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.source"
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[col1])]"
    testSuccess(query, expected)
  }

  "it should correctly parse simple queries 2" in {
    val query = "INSERT OVERWRITE TABLE db1.dest SELECT t1.col1 FROM db2.source T1"
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[db2.source.col1])]"
    testSuccess(query, expected)
  }

  "it should correctly parse a query with sub-query" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT num FROM (SELECT COUNT(1) as num FROM db2.source) T"
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[num], tableDeps[TableDependency(type=TEMP, name=t, columns[num], tableDeps[db2.source])], colDeps[num])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with aliased columns" in {
    val text: String = """INSERT OVERWRITE TABLE db1.dest SELECT T.col1 as col2 FROM db2.source T"""
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[col2=db2.source.col1], tableDeps[db2.source], colDeps[db2.source.col1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with constant columns" in {
    val text: String = """INSERT OVERWRITE TABLE db1.dest SELECT '0' as col FROM db2.source"""
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[col=0], tableDeps[db2.source])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with constant partitions" in {
    val text: String = """INSERT OVERWRITE TABLE db1.dest PARTITION(part=1) SELECT '0' as col FROM db2.source"""
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[col=0], partitions[part=1], tableDeps[db2.source])]"
    testSuccess(text, expected)
  }

  "it should correctly parse queries with joins" in {
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
    val expected: String = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA], tableDeps[TableDependency(type=TEMP, name=!.semijoin, tableDeps[db10.tutu, db2.tutu, db3.tutu, db4.tutu, db5.tutu, db6.tutu, db7.tutu, db8.tutu, db9.tutu], colDeps[db10.tutu.id, db2.tutu.id]), db11.tutu, db2.tutu, db3.tutu, db4.tutu, db5.tutu, db6.tutu, db7.tutu, db8.tutu, db9.tutu], colDeps[colA, db11.tutu.id, db2.tutu.id, db3.tutu.id, db4.tutu.id, db5.tutu.id, db6.tutu.id, db7.tutu.id, db8.tutu.id, db9.tutu.id])]"
    testSuccess(query, expected)
  }

  "a query with where clauses should be correctly parsed" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM db2.tata AS T1
        | WHERE T1.day < '2'
        | AND true
        | OR false
        | AND true = true
        | OR false = false
        | AND NOT true
        | AND !true
        | AND 0 IN (1, 2, 3)
        | AND 0 NOT IN (4, 5, 6)
        | AND 'a' > 'b'
        | AND 1 >= 2
        | AND 3 == 4
        | AND 5 < 6
        | AND 7 <= 8
        | AND 9 <> 10
        | AND 11 != 12
        | AND 13 <=> 14
        | AND 15 BETWEEN 16 AND 17
        | AND "a" LIKE "b"
        | AND "c" RLIKE "d"
        | AND "e" REGEXP "f"
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA], tableDeps[db2.tata], colDeps[colA, db2.tata.day])]"
    testSuccess(query, expected)
  }

  "it should correctly parse queries with join 1" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM db2.tata AS T1
        | JOIN db3.tutu AS T2
        | ON T1.id=T2.id
        | WHERE T1.day < '2'
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA], tableDeps[db2.tata, db3.tutu], colDeps[colA, db2.tata.day, db2.tata.id, db3.tutu.id])]"
    testSuccess(query, expected)
  }

  "it should correctly parse queries with join 2" in {
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
    val expected = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA, colB], tableDeps[db3.tutu, TableDependency(type=TEMP, name=t2, columns[colA=t1.a, colB=b], tableDeps[TableDependency(type=TEMP, name=t1, columns[cA=db2.tata.a, b], tableDeps[db2.tata], colDeps[db2.tata.a, db2.tata.b])], colDeps[b, t1.a])], colDeps[colA, colB, db3.tutu.a, db3.tutu.day])]"
    testSuccess(query, expected)
  }

  "it should correctly parse queries with LEFT SEMI JOIN without a join clause" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM db2.tata AS T1
        | LEFT SEMI JOIN db3.tutu AS T2
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA], tableDeps[TableDependency(type=TEMP, name=!.semijoin, tableDeps[db2.tata, db3.tutu]), db2.tata], colDeps[colA])]"
    testSuccess(query, expected)
  }

  "it should correctly parse queries with LEFT SEMI JOIN with a join clause" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.Toto
        | SELECT colA
        | FROM db2.tata AS T1
        | LEFT SEMI JOIN db3.tutu AS T2
        | ON T1.colA = T2.colA
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA], tableDeps[TableDependency(type=TEMP, name=!.semijoin, tableDeps[db2.tata, db3.tutu], colDeps[db2.tata.colA, db3.tutu.colA]), db2.tata], colDeps[colA])]"
    testSuccess(query, expected)
  }


  "it should correctly parse a query with WHERE col IN (subquery)" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT a FROM db2.source1
        | WHERE b IN (SELECT c FROM db2.source2)
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[a], tableDeps[TableDependency(type=TEMP, name=!.subquery_expr, tableDeps[db2.source2], colDeps[c]), db2.source1], colDeps[a, b])]"
    testSuccess(query, expected)
  }

  "it should correctly parse a query with WHERE col IN (subquery) and an external column used in the subquery" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT a FROM db2.source1 T
        | WHERE T.b IN (SELECT c FROM db2.source2 WHERE c = T.c)
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[a], tableDeps[TableDependency(type=TEMP, name=!.subquery_expr, tableDeps[db2.source2], colDeps[c, db2.source1.c]), db2.source1], colDeps[a, db2.source1.b])]"
    testSuccess(query, expected)
  }

  "WHERE col IN (subquery): column from external tables can only be used in the WHERE clause of the subquery" ignore {
    /* This is forbidden in Hive 2.0 */
    val query: String =
    """SELECT
      |1
      |FROM test.table1 T1
      |WHERE 1=1 AND
      |T1.id IN (
      |  SELECT T2.id
      |  FROM test.table2 T2
      |  JOIN test.table3 T3
      |  ON T2.id = T3.id
      |  WHERE T2.id = T1.id
      |)
      |;
      |""".stripMargin
    testSuccess(query, null)
  }

  "it should correctly parse a query with WHERE EXISTS (subquery)" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT a FROM db2.source1
        | WHERE EXISTS (SELECT c FROM db2.source2)
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[a], tableDeps[TableDependency(type=TEMP, name=!.subquery_expr, tableDeps[db2.source2], colDeps[c]), db2.source1], colDeps[a])]"
    testSuccess(query, expected)
  }

  "it should correctly parse query with partition" in {
    val query: String = "INSERT OVERWRITE TABLE db1.Toto PARTITION(coco='hello') SELECT colA FROM db2.tata AS T1"
    val expected: String = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA], partitions[coco=hello], tableDeps[db2.tata], colDeps[colA])]"
    testSuccess(query, expected)
  }

  "it should correctly parse a query with column alias in subquery" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT num FROM (SELECT COUNT(1) as num FROM db2.source) T"
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[num], tableDeps[TableDependency(type=TEMP, name=t, columns[num], tableDeps[db2.source])], colDeps[num])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with column alias in subquery (bis)" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT T.c FROM (SELECT source.col1 as c FROM db2.Source) T"
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[c], tableDeps[TableDependency(type=TEMP, name=t, columns[c=db2.source.col1], tableDeps[db2.source], colDeps[db2.source.col1])], colDeps[t.c])]"
    testSuccess(text, expected)
  }

  "When querying on a struct object eg (very.big.object) the dependency column is very.big.object and the name given by Hive to the column will be object" in {
    val query: String = "INSERT OVERWRITE TABLE db1.dest SELECT very.big.object FROM db2.source T"
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[object=very.big.object], tableDeps[db2.source], colDeps[very.big.object])]"
    testSuccess(query, expected)
  }

  "When querying on a map" in {
    val text: String = """INSERT OVERWRITE TABLE db1.dest PARTITION(part=1) SELECT col["Hello"] FROM db2.source"""
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[_c0], partitions[part=1], tableDeps[db2.source], colDeps[col])]"
    testSuccess(text, expected)
  }

  "When querying on a map of struct" in {
    val text: String = """INSERT OVERWRITE TABLE db1.dest PARTITION(part=1) SELECT col["Hello"].value FROM db2.source"""
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[value], partitions[part=1], tableDeps[db2.source], colDeps[col])]"
    testSuccess(text, expected)
  }


  "As of Hive 1.1.0, the expected behavior here is to refuse this syntax" in {
    val query: String = "CREATE VIEW db2.view AS SELECT * as col1 FROM db1.source"
    testFailure(query, isView = true)
  }

  "it should correctly parse a query with GROUP BY on computed column" in {
    val query: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT f(col) as fc, count(*)
        | FROM table_name
        | GROUP BY fc
        | ORDER BY fc""".stripMargin
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[fc, _c1], tableDeps[table_name], colDeps[col, fc], postColDeps[fc])]"
    testSuccess(query, expected)
  }

  "it should correctly parse a regular query with DISTRIBUTE BY" in {
    val text: String =
      """
        | INSERT OVERWRITE TABLE user_model.daily_users
        | SELECT
        |   col1 as col2
        | FROM db_source.table_1
        | DISTRIBUTE BY col2, col3
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=daily_users, schema=user_model, columns[col2=col1], tableDeps[db_source.table_1], colDeps[col1], postColDeps[col2, col3])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a regular query with ORDER BY" in {
    val text: String =
      """
        | INSERT OVERWRITE TABLE user_model.daily_users
        | SELECT
        |   col1 as col2
        | FROM db_source.table_1
        | ORDER BY col2, col3
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=daily_users, schema=user_model, columns[col2=col1], tableDeps[db_source.table_1], colDeps[col1], postColDeps[col2, col3])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a regular query with WINDOWING" in {
    val text: String =
      """
        | INSERT OVERWRITE TABLE user_model.daily_users
        | SELECT
        |   RANK() OVER (ORDER BY col1) as col2
        | FROM db_source.table_1
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=daily_users, schema=user_model, columns[col2], tableDeps[db_source.table_1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a regular query with HAVING" in {
    val text: String =
      """
        | INSERT OVERWRITE TABLE user_model.daily_users
        | SELECT
        |   day,
        |   COUNT(1) as nb
        | FROM db_source.table1
        | GROUP BY day
        | HAVING nb > 10
        | """.stripMargin
    val expected: String = "[TableDependency(type=REF, name=daily_users, schema=user_model, columns[day, nb], tableDeps[db_source.table1], colDeps[day], bothColDeps[nb])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a regular query (bis)" in {
    val text: String =
      """ INSERT OVERWRITE TABLE toto
        | SELECT colA, colB  FROM (
        |     FROM (SELECT tata.a as cA, tata.b FROM tata) `T1`
        |     SELECT T1.a as colA, b as colB
        | ) `T2`
        | JOIN db.tutu `T3` ON T3.a=T3.a
        | WHERE T3.day BETWEEN date_sub('2',2) AND '2'""".stripMargin
    val expected = "[TableDependency(type=REF, name=toto, columns[colA, colB], tableDeps[db.tutu, TableDependency(type=TEMP, name=t2, columns[colA=t1.a, colB=b], tableDeps[TableDependency(type=TEMP, name=t1, columns[cA=tata.a, b], tableDeps[tata], colDeps[tata.a, tata.b])], colDeps[b, t1.a])], colDeps[colA, colB, db.tutu.a, db.tutu.day])]"
    testSuccess(text, expected)
  }

  "it should correctly parse text with multiple queries" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT col1 FROM db2.source1 ; INSERT INTO TABLE db1.dest SELECT col1 FROM db2.source2"
    val expected: String =
      "[" +
      "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source1], colDeps[col1]), " +
      "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source2], colDeps[col1])" +
      "]"
    testSuccess(text, expected)
  }

  // When querying on a struct object eg (very.big.object) the dependency column is very.big.object and the name given by Hive to the column will be object.
  "it should correctly parse StructColumns" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT very.big.object FROM db2.source T"
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[object=very.big.object], tableDeps[db2.source], colDeps[very.big.object])]"
    testSuccess(text, expected)
  }

  "it should correctly parse CREATE VIEW" in {
    val text: String =
      """ CREATE VIEW test.view AS
        | SELECT MAX(c1) as col1, MAX(c2), T.c2, c3
        | FROM (SELECT a+b as c1, c+d as c2, e+f as c3 FROM db.source) T
        | """.stripMargin
    val expected: String = "[TableDependency(type=VIEW, name=view, schema=test, columns[col1, _c1, c2, c3], tableDeps[TableDependency(type=TEMP, name=t, columns[c1, c2, c3], tableDeps[db.source], colDeps[a, b, c, d, e, f])], colDeps[c1, c2, c3, t.c2])]"
    testSuccess(text, expected, isView = true)
  }

  "it should correctly parse CREATE VIEW ... AS SELECT *" in {
    val query: String = "CREATE VIEW test.view AS SELECT 'a', *, 'b', `T`.*  FROM (SELECT a+b as col1, c+d as col2 FROM db.source) T"
    val expected: String = "[TableDependency(type=VIEW, name=view, schema=test, columns[_c0=a, *, _c2=b, t.*], tableDeps[TableDependency(type=TEMP, name=t, columns[col1, col2], tableDeps[db.source], colDeps[a, b, c, d])], colDeps[*, t.*])]"
    testSuccess(query, expected, isView = true)
  }

  "it should correctly parse CREATE VIEW ... AS with a subQueryExpr" in {
    val query: String =
      """CREATE VIEW db1.view
        |AS
        |SELECT * FROM (SELECT 1 as client_id) A
        |WHERE A.client_id NOT IN (SELECT 1 as client_id)
        |""".stripMargin
    val expected: String = "[TableDependency(type=VIEW, name=view, schema=db1, columns[*], tableDeps[TableDependency(type=TEMP, name=a, columns[client_id=1])], colDeps[*, a.client_id])]"
    testSuccess(query, expected, isView = true)
  }

  "it should correctly parse CREATE VIEW IF NOT EXISTS ... AS SELECT *" in {
    val query: String = "CREATE VIEW IF NOT EXISTS test.view AS  SELECT 'a', *, 'b', `T`.* FROM (SELECT a+b as col1, c+d as col2 FROM db.source) T"
    val expected: String = "[TableDependency(type=VIEW, name=view, schema=test, columns[_c0=a, *, _c2=b, t.*], tableDeps[TableDependency(type=TEMP, name=t, columns[col1, col2], tableDeps[db.source], colDeps[a, b, c, d])], colDeps[*, t.*])]"
    testSuccess(query, expected, isView = true)
  }

  "it should correctly parse query with MapReduce" in {
    val text: String =
      """ INSERT OVERWRITE TABLE dbm_reports_temp.temp_id_red_result
        | REDUCE * using ''
        | AS colA, colB, colC
        | FROM (SELECT * FROM db2.source) mapped ;""".stripMargin
    val expected = "[TableDependency(type=REF, name=temp_id_red_result, schema=dbm_reports_temp, columns[colA, colB, colC], tableDeps[TableDependency(type=TEMP, name=mapped, columns[*], tableDeps[db2.source], colDeps[*])], colDeps[*])]"
    testSuccess(text, expected)
  }

  "an incorrect query with UNION ALL should be NOT OK" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM
        |(
        |  SELECT col1, col2 FROM db2.source1
        |  UNION ALL
        |  SELECT col1 FROM db2.source2
        |) T""".stripMargin
    testFailure(text)
  }

  "it should correctly parse queries with UNION ALL" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1, col2, col3, col4 FROM db2.source1 T1
        |UNION ALL
        |SELECT col1, col2, col3, col4 FROM db2.source2 T2
        |UNION ALL
        |SELECT col1, col2, col3, col4 FROM db2.source3
        |""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[*], tableDeps[TableDependency(type=TEMP, name=_u1, columns[*], tableDeps[TableDependency(type=TEMP, name=!.union0, columns[col1, col2, col3, col4], tableDeps[db2.source3], colDeps[col1, col2, col3, col4]), TableDependency(type=TEMP, name=!.union1, columns[col1, col2, col3, col4], tableDeps[db2.source1], colDeps[col1, col2, col3, col4]), TableDependency(type=TEMP, name=!.union2, columns[col1, col2, col3, col4], tableDeps[db2.source2], colDeps[col1, col2, col3, col4])], colDeps[!.union0.*, !.union1.*, !.union2.*])], colDeps[*])]"
    testSuccess(text, expected)
  }

  "it should correctly parse queries with UNION ALL in subquery" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT T.col1 FROM
        |(
        |SELECT T1.* FROM db2.source1 T1
        |UNION ALL
        |SELECT T2.col1 FROM db2.source2 T2
        |UNION ALL
        |SELECT col1 FROM db2.source3
        |) T""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[*], tableDeps[TableDependency(type=TEMP, name=!.union0, columns[col1], tableDeps[db2.source3], colDeps[col1]), TableDependency(type=TEMP, name=!.union1, columns[db2.source1.*], tableDeps[db2.source1], colDeps[db2.source1.*]), TableDependency(type=TEMP, name=!.union2, columns[col1], tableDeps[db2.source2], colDeps[db2.source2.col1])], colDeps[!.union0.*, !.union1.*, !.union2.*])], colDeps[t.col1])]"
    testSuccess(text, expected)
  }

  "a query with UNION ALL and stars should be OK bis" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT * FROM
        |(
        |  SELECT * FROM db2.source1
        |  UNION ALL
        |  SELECT * FROM db2.source2
        |) T
        |""".stripMargin
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[*], tableDeps[TableDependency(type=TEMP, name=t, columns[*], tableDeps[TableDependency(type=TEMP, name=!.union0, columns[*], tableDeps[db2.source1], colDeps[*]), TableDependency(type=TEMP, name=!.union1, columns[*], tableDeps[db2.source2], colDeps[*])], colDeps[!.union0.*, !.union1.*])], colDeps[*])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with sub-query and the same table alias used inside the subquery." in {
    val query = "INSERT OVERWRITE TABLE db1.toto SELECT colA, colB FROM (SELECT a as cA, b FROM db2.tata T1) T1"
    val expected = "[TableDependency(type=REF, name=toto, schema=db1, columns[colA, colB], tableDeps[TableDependency(type=TEMP, name=t1, columns[cA=a, b], tableDeps[db2.tata], colDeps[a, b])], colDeps[colA, colB])]"
    testSuccess(query, expected)
  }

  "column name checking" - {
    "it should fail on a query with the same column name used twice" in {
      val text: String =
        """
          |INSERT OVERWRITE TABLE db1.dest
          |SELECT
          |  T1.c1 c1,
          |  col2 as c1
          |FROM db2.source1
          |""".stripMargin
      testFailure(text)
    }
  }

  "it should fail on query with the same alias used twice" in {
    val text: String = "INSERT OVERWRITE TABLE db1.dest SELECT col2 FROM db2.source1 T JOIN db2.source2 T"
    testFailure(text)
  }

  "it should correctly parse a query with a CTE 1" in {
    val text: String =
      """WITH T AS (SELECT col1 FROM db2.source)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM (SELECT col1 FROM T) T
        |""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[col1], tableDeps[db2.source], colDeps[col1])], colDeps[col1])], colDeps[col1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with a CTE 2" in {
    val text: String =
      """WITH T AS (SELECT col1 FROM db2.source)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM (SELECT T.col1 FROM T2) T
        |""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[col1=T.col1], tableDeps[t2], colDeps[T.col1])], colDeps[col1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with two CTES" in {
    val text: String =
      """WITH T AS (SELECT col1 FROM db2.source), T2 AS (SELECT col1 FROM db2.source)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM (SELECT col1, col2 FROM T) T
        |""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[col1, col2], tableDeps[TableDependency(type=TEMP, name=t, columns[col1], tableDeps[db2.source], colDeps[col1])], colDeps[col1, col2])], colDeps[col1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with a SEMIJOIN" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT *
        |FROM db1.source1 A
        |WHERE A.col1 NOT IN (SELECT col1 FROM db1.source2)
        |""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[*], tableDeps[TableDependency(type=TEMP, name=!.subquery_expr, tableDeps[db1.source2], colDeps[col1]), db1.source1], colDeps[*, db1.source1.col1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with a SEMIJOIN in a subquery" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1
        |FROM (
        |  SELECT col1
        |  FROM db1.source1 T1
        |  WHERE T1.col1 IN (
        |    SELECT col1 FROM db1.source2
        |  )
        |) T2
        |""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t2, columns[col1], tableDeps[TableDependency(type=TEMP, name=!.subquery_expr, tableDeps[db1.source2], colDeps[col1]), db1.source1], colDeps[col1, db1.source1.col1])], colDeps[col1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with a SEMIJOIN in a CTE" in {
    val text: String =
      """WITH T2 AS (
        |  SELECT col1 FROM db1.source2 T1
        |)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1
        |FROM (
        |  SELECT col1
        |  FROM db1.source1 T1
        |  WHERE T1.col1 IN (
        |    SELECT col1 FROM T2
        |  )
        |) T2
        |""".stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t2, columns[col1], tableDeps[TableDependency(type=TEMP, name=!.subquery_expr, tableDeps[TableDependency(type=TEMP, name=t2, columns[col1], tableDeps[db1.source2], colDeps[col1])], colDeps[col1]), db1.source1], colDeps[col1, db1.source1.col1])], colDeps[col1])]"
    testSuccess(text, expected)
  }

  "it should include columns in where clauses only as colDeps" in {
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT col1 FROM db2.source WHERE col2 >= col3 """.stripMargin
    val expected = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[db2.source], colDeps[col1, col2, col3])]"
    testSuccess(text, expected)
  }

  "it should fail on query with the same alias used twice in two CTEs" in {
    val text: String =
      """ WITH T AS (SELECT col1 FROM db2.source1), T AS (SELECT col1 FROM db2.source1)
        | INSERT OVERWRITE TABLE db1.dest
        | SELECT col2 FROM T""".stripMargin
    testFailure(text)
  }

  "it should fail on query with the same alias used twice in two subqueries" in {
    val text: String =
      """ INSERT OVERWRITE TABLE db1.dest
        | SELECT col2 FROM (SELECT col1 FROM db2.source1) T
        | JOIN (SELECT col2 FROM db2.source2) T""".stripMargin
    testFailure(text)
  }

  "it should correctly parse a query with a CTE and UNION" in {
    val text: String =
      """WITH T AS (SELECT col1 FROM db2.source1)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col1
        |FROM (
        |   SELECT col1 FROM T
        |   UNION ALL
        |   SELECT col1 FROM T
        |) T2
        |""".stripMargin
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t2, columns[*], tableDeps[TableDependency(type=TEMP, name=!.union0, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[col1], tableDeps[db2.source1], colDeps[col1])], colDeps[col1]), TableDependency(type=TEMP, name=!.union1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[col1], tableDeps[db2.source1], colDeps[col1])], colDeps[col1])], colDeps[!.union0.*, !.union1.*])], colDeps[col1])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with a CTE and a LATERAL VIEW" in {
    val text: String =
      """WITH T AS (SELECT col1 FROM db2.source1)
        |INSERT OVERWRITE TABLE db1.dest
        |SELECT col2
        |FROM T
        |LATERAL VIEW explode(col1) LV as col2
        |""".stripMargin
    val expected: String = "[TableDependency(type=REF, name=dest, schema=db1, columns[col2], tableDeps[TableDependency(type=TEMP, name=lv, columns[col2], tableDeps[TableDependency(type=TEMP, name=t, columns[col1], tableDeps[db2.source1], colDeps[col1])], colDeps[col1]), TableDependency(type=TEMP, name=t, columns[col1], tableDeps[db2.source1], colDeps[col1])], colDeps[col2])]"
    testSuccess(text, expected)
  }

  "it should correctly parse a query with a JOIN and a LATERAL VIEW" ignore {
    /* This syntax is recognized by Spark 2.0 but not by Hive 2.0 */
    val text: String =
      """INSERT OVERWRITE TABLE db1.dest
        |SELECT LV.c1
        |FROM T1
        |JOIN T2
        |ON T1.c1 = T2.c1
        |LATERAL VIEW explode(T1.c2) LV as c1
        |""".stripMargin
    val expected: String = null
    testSuccess(text, expected)
  }

  "it should correctly parse a query with a multi-insert in the same table" in {
    val text: String =
      """FROM (SELECT col1, col2, col3, col4 FROM db2.source) T
        |INSERT OVERWRITE TABLE db1.dest SELECT col1 WHERE col2 > 0
        |INSERT OVERWRITE TABLE db1.dest SELECT col3 WHERE col4 < 0
        |""".stripMargin
    val expected =
      "[" +
      "TableDependency(type=REF, name=dest, schema=db1, columns[col1], tableDeps[TableDependency(type=TEMP, name=t, columns[col1, col2, col3, col4], tableDeps[db2.source], colDeps[col1, col2, col3, col4])], colDeps[col1, col2]), " +
      "TableDependency(type=REF, name=dest, schema=db1, columns[col3], tableDeps[TableDependency(type=TEMP, name=t, columns[col1, col2, col3, col4], tableDeps[db2.source], colDeps[col1, col2, col3, col4])], colDeps[col3, col4])" +
      "]"
    testSuccess(text, expected)
  }

  "handleException should correctly work" in {
    val query: String =
      """Hello
        |World
        |Good
        |Weather
        |Today!""".stripMargin
    val message: String = """line 2:13 cannot recognize input near '""' 'GROUP' 'BY' in expression specification"""
    val exception = FlamyParsingException(query, new FlamyException(message), verbose = false)
    val actual: String = exception.getMessage
    val expected: String =
      """line 2:13 cannot recognize input near '""' 'GROUP' 'BY' in expression specification
        |-   Hello
        |>>> World
        |-   Good""".stripMargin
    assert(expected === actual)
  }

  "column checking : a table" - {
    "with multiple column inserted with the same name should be ok" ignore {
      // TODO: this case should throw a warning or a failure (and it should be configurable)
      val text: String =
        """INSERT OVERWRITE TABLE T1 PARTITION(p1, p2)
          |SELECT
          |  c1 as c1,
          |  c2 as c1
          |FROM db2.source T2
          |""".stripMargin
      testSuccess(text, null)
    }
  }


}
