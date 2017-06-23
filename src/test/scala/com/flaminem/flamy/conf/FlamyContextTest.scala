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

package com.flaminem.flamy.conf

import com.flaminem.flamy.model.Variables
import com.typesafe.config._
import com.typesafe.config.impl.{ConfigString, Parseable}
import org.scalatest.FunSpec

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by fpin on 12/25/14.
 */
class FlamyContextTest extends FunSpec {

  describe("a FlamyContext") {
    it("should correctly read variables"){
      val context: FlamyContext =
        new FlamyContext(
          "flamy.model.dir.paths" -> "src/test/resources/test",
          "flamy.variables.path" -> "${flamy.model.dir.paths}/VARIABLES.properties"
        )
      val expected: Variables = new Variables
      expected.put("DATE", "\"2014-01-01\"")
      expected.put("LOOKBACK_WINDOW", "90")
      expected.put("SINCE_DAY", "> \"2014-01-01\"")
      expected.put("SC_ARRAY", "a ; b ; c ; d")
      expected.put("COLON_ARRAY", "(a , b , c , d)")
      expected.put("partition:day", "\"2014-01-01\"")
      val actual: Variables = context.getVariables
      assert(expected === actual)
    }
  }

}
