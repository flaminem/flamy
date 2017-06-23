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

import org.scalatest.{FreeSpec, Matchers}

/**
  * Created by fpin on 8/26/16.
  */
class ParsingUtils$Test extends FreeSpec with Matchers {


  "skipStrings" - {

    "should work with string 1" in {
      val s = """''abc"""
      assert( ParsingUtils.skipString(s.toCharArray, 0) === s.length - 3 )
    }

    "should work with string 2" in {
      val s = """'""'abc"""
      assert( ParsingUtils.skipString(s.toCharArray, 0) === s.length - 3 )
    }

    "should work with string 3" in {
      val s = """'"\'"'abc"""
      assert( ParsingUtils.skipString(s.toCharArray, 0) === s.length - 3 )
    }

  }

  "skipParentheses" - {

    "should work with parentheses" in {
      val s = "((()())(()))"
      assert( ParsingUtils.skipParentheses(s.toCharArray, 0) === s.length )
    }

    "should work with parentheses and string" in {
      val s = """((()("'\"'"))(('"\'"')))()"""
      assert( ParsingUtils.skipParentheses(s.toCharArray, 0) === s.length - 2 )
    }

  }

  "noSpaceTableNamePattern should correctly recognise table names" in {
    val regex = ParsingUtils.noSpaceTableNamePattern.r

    assert(regex.findFirstMatchIn("").isEmpty)
    assert(regex.findFirstMatchIn(",T").nonEmpty)
    assert(regex.findFirstMatchIn(",db.T").nonEmpty)
    assert(regex.findFirstMatchIn(",db.T`T2`").nonEmpty)
    assert(regex.findFirstMatchIn("FROM T").nonEmpty)
    assert(regex.findFirstMatchIn("FROM db.t").nonEmpty)
    assert(regex.findFirstMatchIn("FROM`T`").nonEmpty)
    assert(regex.findFirstMatchIn("FROM`db`.`t`").nonEmpty)
    assert(regex.findFirstMatchIn("FROM`db`.t").nonEmpty)
    assert(regex.findFirstMatchIn("FROM db.`t`").nonEmpty)
    assert(regex.findFirstMatchIn("FROM `db`.`t`").nonEmpty)

    val r = ParsingUtils.noSpaceTableNamePattern.r

    assert(r.findFirstMatchIn(",db4.toto`t2`,db5.toto)").get.group(0) === "db4.toto")
  }

  "tableNamePattern should correctly recognise table names" in {
    val regex = ParsingUtils.tableNamePattern.r

    assert(regex.findFirstMatchIn("").isEmpty)
    assert(regex.findFirstMatchIn("FROM T").nonEmpty)
    assert(regex.findFirstMatchIn("FROM db.t").nonEmpty)
    assert(regex.findFirstMatchIn("FROM`T`").nonEmpty)
    assert(regex.findFirstMatchIn("FROM`db`.`t`").nonEmpty)
    assert(regex.findFirstMatchIn("FROM`db`.t").nonEmpty)
    assert(regex.findFirstMatchIn("FROM db.`t`").nonEmpty)
    assert(regex.findFirstMatchIn("FROM `db`.`t`").nonEmpty)

    assert(regex.findFirstMatchIn(" db1.test").get.group(0) === " db1.test")

    assert(regex.findFirstMatchIn(",T").isEmpty)
  }

}
