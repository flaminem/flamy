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

import org.scalatest.FunSuite

/**
 * Created by fpin on 8/1/15.
 */
class TableTest extends FunSuite {

  test("a table should be correctly printed") {
    val table = new Table(TableType.TABLE,"test","table",Seq(new Column("col1")))
    assert(table.toString === "Table(type=TABLE, name=test, schema=table, columns[col1])")
  }

}
