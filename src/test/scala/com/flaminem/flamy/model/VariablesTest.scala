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

import org.rogach.scallop.{ScallopConf, Subcommand}
import org.scalatest.{FreeSpec, Matchers}

/**
 * Created by fpin on 5/25/15.
 */
class VariablesTest extends FreeSpec with Matchers{

  "replaceInText should work" in {
    val variables = new Variables
    variables += ("TO_REPLACE" -> "REPLACED")
    val text = "TO_REPLACE has been ${TO_REPLACE}"
    val expected = "TO_REPLACE has been REPLACED"

    assert(variables.replaceInText(text)===expected)
  }

  "subsetInText should work" in {
    val variables = new Variables
    variables += ("IN_KEY" -> "IN_VALUE")
    variables += ("OUT_KEY" -> "OUT_VALUE")
    val text = "this text contains ${IN_KEY} but does not contains OUT_KEY"

    val expectedVariables = new Variables
    expectedVariables += ("IN_KEY" -> "IN_VALUE")

    assert(variables.subsetInText(text, Nil) === expectedVariables)
  }

  "replaceInText should preserve partition variables" in {
    val text: String = """INSERT OVERWRITE TABLE db1.dest PARTITION(part=${partition:toto}) SELECT ${partition:toto} as num FROM db2.source"""
    val vars = new Variables()
    vars += "partition:toto" -> "${partition:toto}0"
    val expected: String = """INSERT OVERWRITE TABLE db1.dest PARTITION(part="${partition:toto}") SELECT "${partition:toto}" as num FROM db2.source"""
    assert(vars.replaceInText(text) == expected)
  }


  "the scallopConverter should work" in {
    object Conf extends ScallopConf(Seq("--variables", "HELLO=yes", "LIST=(1,2,3)", "sub")) {
      val variables = opt[Variables](name = "variables")(Variables.scallopConverter)
      val sub =
        new Subcommand("sub") {
          banner("Print version information of this program")
          override def toString = "sub"
        }
    }
    assert(Conf.variables() === Variables("HELLO" -> "yes", "LIST" -> "(1,2,3)"))
    assert(Conf.subcommand.get.toString === "sub")
  }

}
