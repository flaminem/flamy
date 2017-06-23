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
import com.flaminem.flamy.parsing.hive.FlamyParsingException
import com.flaminem.flamy.parsing.model.ColumnDependency
import org.scalatest.FreeSpec

/**
  * Created by fpin on 12/10/16.
  */
class WhereASTTest extends FreeSpec {

  implicit val flamyContext = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")

  "Methods tests" - {
    "toAST" in {
      val clause = """c1 = "A" AND source.c2 = 2 AND db.source.c3 = 3"""
      val whereAST = WhereAST(clause)
      assert (SQLFormatter.treeToSQL(whereAST.tree) === clause)

    }

    "getUsedColumns should work" in {
      val whereAST = WhereAST("""c1 = "A" AND source.c2 = 2 AND db.source.c3 = 3""")
      val expected =
        Seq(
          new ColumnDependency("c1"),
          new ColumnDependency("c2", "source"),
          new ColumnDependency("c3", "source", "db")
        )
      assert(whereAST.usedColumns === expected)
    }

    "getUsedColumns should work with quoted names" in {
      val whereAST = WhereAST("""1 = `db.source`.c3""")
      val expected =
        Seq(
          new ColumnDependency("c3", "db.source")
        )
      assert(whereAST.usedColumns === expected)
    }

    "getUsedColumns should throw an exception when table name is incorrect" in {
      intercept[FlamyParsingException]{
        WhereAST("""1 = db.source.c3.truc""")
      }
    }


  }


}
