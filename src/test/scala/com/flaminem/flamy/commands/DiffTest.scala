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

package com.flaminem.flamy.commands

import com.flaminem.flamy.conf.FlamyContext
import org.scalatest.{FreeSpec, Matchers}

/**
  * Created by fpin on 8/24/16.
  */
class DiffTest extends FreeSpec with Matchers {

  "diffColumns should work" in {
    val leftContext = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/LeftDiff")
    val rightContext = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/RightDiff")

    val diff = new Diff

    val expected =
      """+-----------------------+-----------------------+
        || model                 | model                 |
        |+-----------------------+-----------------------+
        || db_source.left_table  |                       |
        ||                       | db_source.right_table |
        || db_source.source      | db_source.source      |
        ||     col2 INT          |                       |
        ||     * partcol2 STRING |     * partcol3 STRING |
        |+-----------------------+-----------------------+""".stripMargin
    assert(diff.diffColumns(leftContext, rightContext) === expected)
  }

  "diffTables should work" in {
    val leftContext = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/LeftDiff")
    val rightContext = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/RightDiff")

    val diff = new Diff
    val expected =
      """+----------------------+-----------------------+
        || model                | model                 |
        |+----------------------+-----------------------+
        || db_source.left_table |                       |
        ||                      | db_source.right_table |
        |+----------------------+-----------------------+""".stripMargin
    assert(diff.diffTables(leftContext, rightContext) === expected)

  }

}
