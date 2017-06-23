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

package com.flaminem.flamy.utils

import org.scalatest.FreeSpec

/**
  * Created by fpin on 2/14/17.
  */
class CliUtilsTest extends FreeSpec {

  "removeBackSlashes method" - {

    "should correctly remove backslashes before simple quotes between simple quotes" in {
      assert(CliUtils.removeBackSlashes("""\'a\'""", "'") === """'a'""")
    }

    "should not remove backslashes before simple quotes between double quotes" in {
      assert(CliUtils.removeBackSlashes("""\'a\'""", "\"") === """\'a\'""")
    }

    "should correctly remove backslashes before double quotes between double quotes" in {
      assert(CliUtils.removeBackSlashes("""\"a\"""", "\"") === """"a"""")
    }

    "should not remove backslashes before double quotes between simple quotes" in {
      assert(CliUtils.removeBackSlashes("""\"a\"""", "'") === """\"a\"""")
    }

    "should correctly remove backslashes before backslashes between double quotes" in {
      assert(CliUtils.removeBackSlashes("""a\\b""", "\"") === """a\b""")
    }

    "should correctly remove backslashes before backslashes between simple quotes" in {
      assert(CliUtils.removeBackSlashes("""a\\b""", "'") === """a\b""")
    }

  }

  "split method" - {
    "should correctly split a simple string" in {
      val s = """This is a simple test"""
      val expected = Seq("This", "is", "a", "simple", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with double quotes" in {
      val s = """This is a "less simple" test"""
      val expected = Seq("This", "is", "a", "less simple", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with multiple double quotes" in {
      val s = """This is a "much ""less"" simple" test"""
      val expected = Seq("This", "is", "a", "much less simple", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with simple quotes" in {
      val s = """This is a 'less simple' test"""
      val expected = Seq("This", "is", "a", "less simple", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with multiple simple quotes" in {
      val s = """This is a 'much ''less'' simple' test"""
      val expected = Seq("This", "is", "a", "much less simple", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with both double and simple quotes" in {
      val s = """This is an 'even '"much "'less '"simple" test"""
      val expected = Seq("This", "is", "an", "even much less simple", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with simple quotes inside double quotes" in {
      val s = """This is a "'simple'" test"""
      val expected = Seq("This", "is", "a", "'simple'", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with double quotes inside simple quotes" in {
      val s = """This is a '"simple"' test"""
      val expected = Seq("This", "is", "a", "\"simple\"", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with backslashed double quotes inside double quotes" in {
      val s = """This is a "\"simple\"" test"""
      val expected = Seq("This", "is", "a", "\"simple\"", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with backslashed simple quotes inside simple quotes" in {
      val s = """This is a '\'simple\'' test"""
      val expected = Seq("This", "is", "a", "'simple'", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with backslashed simple quotes inside double quotes" in {
      val s = """This is a "\'simple\'" test"""
      val expected = Seq("This", "is", "a", """\'simple\'""", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with backslashed double quotes inside simple quotes" in {
      val s = """This is a '\"simple\"' test"""
      val expected = Seq("This", "is", "a", """\"simple\"""", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with double backslashes inside simple quotes" in {
      val s = """This is a '\\weird\\' test"""
      val expected = Seq("This", "is", "a", """\weird\""", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with double backslashes inside double quotes" in {
      val s = """This is a "\\weird\\" test"""
      val expected = Seq("This", "is", "a", """\weird\""", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string with triple backslashes inside double quotes" in {
      val s = """This is a "\\\very weird\\\"" test"""
      val expected = Seq("This", "is", "a", """\\very weird\"""", "test")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string that ends with 1 whitespace (annoying, but required for autocomplete)" in {
      val s = """This is a simple test """
      val expected = Seq("This", "is", "a", "simple", "test", "")
      assert(CliUtils.split(s) === expected)
    }

    "should correctly split a string that ends with 3 whitespaces (annoying, but required for autocomplete)" in {
      val s = """This is a simple test   """
      val expected = Seq("This", "is", "a", "simple", "test", "")
      assert(CliUtils.split(s) === expected)
    }

  }


}
