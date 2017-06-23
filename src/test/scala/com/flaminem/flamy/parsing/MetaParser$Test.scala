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

package com.flaminem.flamy.parsing

import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.parsing.model.TableDependency
import org.scalatest.FunSuite

/**
 * Created by fpin on 5/25/15.
 */
class MetaParser$Test extends FunSuite{


  test("test") {
    val filePath: String = this.getClass.getClassLoader.getResource("test/db_dest.db/meta_table/META.properties").getFile
    val t: TableDependency = MetaParser.parseTableDependencies(filePath)
    val expected: String = "TableDependency(type=EXT, name=meta_table, schema=db_dest, tableDeps[db_out.table_out1, db_out.table_out2])"
    assert(expected === t.toString)
  }

  test("testParseVariables") {
    val filePath: String = this.getClass.getClassLoader.getResource("test/VARIABLES.properties").getFile
    val actual: Variables = MetaParser.parseVariables(filePath)
    val expected: Variables = new Variables
    expected.put("DATE", "\"2014-01-01\"")
    expected.put("LOOKBACK_WINDOW", "90")
    expected.put("SINCE_DAY", "> \"2014-01-01\"")
    expected.put("SC_ARRAY", "a ; b ; c ; d")
    expected.put("COLON_ARRAY", "(a , b , c , d)")
    expected.put("partition:day", "\"2014-01-01\"")
    assert(expected === actual)
  }

}
