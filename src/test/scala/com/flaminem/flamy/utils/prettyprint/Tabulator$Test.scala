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

package com.flaminem.flamy.utils.prettyprint

import org.scalatest.FunSuite

/**
 * Created by fpin on 12/9/14.
 */
class Tabulator$Test extends FunSuite {

  test("simple test") {
    val table = Seq(
      Seq("First Name","Last Name","Birthday"),
      Seq("John","Smith","1987-09-13"),
      Seq("Michel","Martin","1965-02-18")
    )

    val expected =
      """+------------+-----------+------------+
        || First Name | Last Name |   Birthday |
        |+------------+-----------+------------+
        ||       John |     Smith | 1987-09-13 |
        ||     Michel |    Martin | 1965-02-18 |
        |+------------+-----------+------------+""".stripMargin

    val actual = Tabulator.format(table)

    assert(actual === expected)
  }

  test("test leftJustified") {
    val table = Seq(
      Seq("First Name","Last Name","Birthday"),
      Seq("John","Smith","1987-09-13"),
      Seq("Michel","Martin","1965-02-18")
    )

    val expected =
      """+------------+-----------+------------+
        || First Name | Last Name | Birthday   |
        |+------------+-----------+------------+
        || John       | Smith     | 1987-09-13 |
        || Michel     | Martin    | 1965-02-18 |
        |+------------+-----------+------------+""".stripMargin

    val actual = Tabulator.format(table, leftJustify = true)

    assert(actual === expected)
  }

}
