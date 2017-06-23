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
import com.flaminem.flamy.conf.hive.ModelHiveContext
import com.flaminem.flamy.model.exceptions.UnexpectedBehaviorException
import com.flaminem.flamy.model.names.TableName
import org.apache.hadoop.hive.ql.parse.{ASTNode, ParseDriver}
import org.scalatest.FreeSpec

/**
  * Created by fpin on 5/18/16.
  */
class HiveParserUtils$Test extends FreeSpec {

  implicit val flamyContext = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")

  "drawTree should work" in {

    val query =
      """
        |INSERT OVERWRITE TABLE T1 PARTITION(day)
        |SELECT c1, c2
        |FROM T2
        |LATERAL VIEW explode(myMap) LV as c1, c2
      """.stripMargin

    val pd: ParseDriver = new ParseDriver
    val tree: ASTNode = pd.parse(query, ModelHiveContext.getLightContext(flamyContext).hiveContext)

    val expected =
      """nil
        |├──TOK_QUERY
        |│  ├──TOK_FROM
        |│  │  └──TOK_LATERAL_VIEW
        |│  │     ├──TOK_SELECT
        |│  │     │  └──TOK_SELEXPR
        |│  │     │     ├──TOK_FUNCTION
        |│  │     │     │  ├──explode
        |│  │     │     │  └──TOK_TABLE_OR_COL
        |│  │     │     │     └──myMap
        |│  │     │     ├──c1
        |│  │     │     ├──c2
        |│  │     │     └──TOK_TABALIAS
        |│  │     │        └──LV
        |│  │     └──TOK_TABREF
        |│  │        └──TOK_TABNAME
        |│  │           └──T2
        |│  └──TOK_INSERT
        |│     ├──TOK_DESTINATION
        |│     │  └──TOK_TAB
        |│     │     ├──TOK_TABNAME
        |│     │     │  └──T1
        |│     │     └──TOK_PARTSPEC
        |│     │        └──TOK_PARTVAL
        |│     │           └──day
        |│     └──TOK_SELECT
        |│        ├──TOK_SELEXPR
        |│        │  └──TOK_TABLE_OR_COL
        |│        │     └──c1
        |│        └──TOK_SELEXPR
        |│           └──TOK_TABLE_OR_COL
        |│              └──c2
        |└──<EOF>
        |""".stripMargin

    assert(HiveParserUtils.drawTree(tree) === expected)
  }


  "getInsertedTableName" - {
    "should work with INSERT INTO" in {
      val query = "INSERT INTO db.table1 SELECT 1 FROM (SELECT 1) T"
      val tree: ASTNode = new ParseDriver().parse(query, ModelHiveContext.getLightContext(flamyContext).hiveContext)
      assert(HiveParserUtils.getInsertedTableName(tree) === TableName("db.table1"))
    }

    "should work with INSERT INTO TABLE" in {
      val query = "INSERT INTO TABLE db.table1 SELECT 1 FROM (SELECT 1) T"
      val tree: ASTNode = new ParseDriver().parse(query, ModelHiveContext.getLightContext(flamyContext).hiveContext)
      assert(HiveParserUtils.getInsertedTableName(tree) === TableName("db.table1"))
    }

    "should work with INSERT OVERWRITE TABLE" in {
      val query = "INSERT OVERWRITE TABLE db.table1 SELECT 1 FROM (SELECT 1) T"
      val tree: ASTNode = new ParseDriver().parse(query, ModelHiveContext.getLightContext(flamyContext).hiveContext)
      assert(HiveParserUtils.getInsertedTableName(tree) === TableName("db.table1"))
    }

    "should work with multi-insert on the same table" in {
      val query =
        """FROM (SELECT 1) T
          |INSERT OVERWRITE TABLE db.table1 PARTITION(p=1)
          |SELECT 1
          |INSERT OVERWRITE TABLE db.table1 PARTITION(p=2)
          |SELECT 2
          |""".stripMargin
      val tree: ASTNode = new ParseDriver().parse(query, ModelHiveContext.getLightContext(flamyContext).hiveContext)
      assert(HiveParserUtils.getInsertedTableName(tree) === TableName("db.table1"))
    }

    "should fail with multi-insert on different tables" in {
      val query =
        """FROM (SELECT 1) T
          |INSERT OVERWRITE TABLE db.table1 PARTITION(p=1)
          |SELECT 1
          |INSERT OVERWRITE TABLE db.table2 PARTITION(p=2)
          |SELECT 2
          |""".stripMargin
      val tree: ASTNode = new ParseDriver().parse(query, ModelHiveContext.getLightContext(flamyContext).hiveContext)
      intercept[FlamyParsingException] {
        HiveParserUtils.getInsertedTableName(tree)
      }
    }

    "should fail if table name is not qualified" in {
      val query = "INSERT OVERWRITE TABLE table1 SELECT 1 FROM (SELECT 1) T"
      val tree: ASTNode = new ParseDriver().parse(query, ModelHiveContext.getLightContext(flamyContext).hiveContext)
      intercept[UnexpectedBehaviorException] {
        HiveParserUtils.getInsertedTableName(tree)
      }
    }
  }


}
