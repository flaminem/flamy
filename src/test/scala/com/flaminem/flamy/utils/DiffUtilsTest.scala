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

import com.flaminem.flamy.utils.DiffUtils._
import org.scalatest.{FunSpec, Matchers}

class DiffUtilsTest extends FunSpec with Matchers {

  describe("DiffUtilsTest.hammingDistance") {

    describe("with swaps allowed") {
      val allowSwaps = true

      it("should be correct when the strings are equals") {
        val s1 = "ABCDEF"
        hammingDistance(s1, s1, allowSwaps) should be(0)
      }

      it("should be correct with different strings") {
        hammingDistance("ABCDEF", "AB", allowSwaps) should be(4)
        hammingDistance("AB", "ABCDEF", allowSwaps) should be(4)
        hammingDistance("ABCDEF", "ABC", allowSwaps) should be(3)
        hammingDistance("ABCDEF", "ABDCEF", allowSwaps) should be(2)
        hammingDistance("AEDCBF", "ABCDEF", allowSwaps) should be(4)
        hammingDistance("ABCDEF", "ABCEF", allowSwaps) should be(1)
        hammingDistance("ACDEF", "ABCDEBF", allowSwaps) should be(2)
      }
    }

    describe("with swaps not allowed") {
      val allowSwaps = false

      it("should be correct when the strings are equals") {
        val s1 = "ABCDEF"
        hammingDistance(s1, s1, allowSwaps) should be(0)
      }

      it("should be correct with different strings") {
        hammingDistance("ABCDEF", "AB", allowSwaps) should be(4)
        hammingDistance("AB", "ABCDEF", allowSwaps) should be(4)
        hammingDistance("ABCDEF", "ABC", allowSwaps) should be(3)
        hammingDistance("ABCDEF", "ABDCEF", allowSwaps) should be(2)
        hammingDistance("AEDCBF", "ABCDEF", allowSwaps) should be(6)
        hammingDistance("ABCDEF", "ABCEF", allowSwaps) should be(1)
        hammingDistance("ACDEF", "ABCDEBF", allowSwaps) should be(2)
      }
    }
  }

  private def testHammingDiff(allowSwaps: Boolean, s1: String, s2: String, expected1: String, expected2: String): Unit = {
    val (l1, l2) = hammingDiff(s1, s2, allowSwaps).unzip
    val actual1 = l1.map{_.getOrElse(" ")}.mkString("")
    val actual2 = l2.map{_.getOrElse(" ")}.mkString("")
    if(allowSwaps) {
      assert(
        hammingDistance(l1, l2, allowReplacements = true) === hammingDistance(s1, s2, allowReplacements = true),
        s""" : the hamming distance of the output sequences should be equal to the hamming distance of the input, got "$actual1" and "$actual2" """
      )
    }
    assert(
      l1.length === l2.length,
      s""" : both output sequences should be of same length, got "$actual1" and "$actual2" """
    )
    (actual1, actual2) should be(expected1, expected2)
  }

  describe("DiffUtilsTest.hammingDiff") {

    describe("with swaps allowed") {
      val allowSwaps = true

      it("should be correct when the strings are equal") {
        val s1 = "ABCDEF"
        testHammingDiff(allowSwaps,s1,s1,s1,s1)
      }

      it("should be correct with different strings") {
        testHammingDiff(
          allowSwaps,
          "ABCDEF",
          "ABC",
          "ABCDEF",
          "ABC   "
        )
        testHammingDiff(
          allowSwaps,
          "ABCDEF",
          "ABDCEF",
          "ABCD EF",
          "AB DCEF"
        )
        testHammingDiff(
          allowSwaps,
          "AEDCBF",
          "ABCDEF",
          "AEDCB F",
          "AB CDEF"
        )
        testHammingDiff(
          allowSwaps,
          "ABCDEF",
          "ABCEF",
          "ABCDEF",
          "ABC EF"
        )
        testHammingDiff(
          allowSwaps,
          "ACDEBF",
          "ABCDEF",
          "A CDEBF",
          "ABCDE F"
        )
      }
    }

    describe("with swaps not allowed") {
      val allowSwaps = false

      it("should be correct when the strings are equal") {
        val s1 = "ABCDEF"
        testHammingDiff(allowSwaps,s1,s1,s1,s1)
      }

      it("should be correct with different strings") {
        testHammingDiff(
          allowSwaps,
          "ABCDEF",
          "ABC",
          "ABCDEF",
          "ABC   "
        )
        testHammingDiff(
          allowSwaps,
          "ABCDEF",
          "ABDCEF",
          "ABCD EF",
          "AB DCEF"
        )
        testHammingDiff(
          allowSwaps,
          "AEDCBF",
          "ABCDEF",
          "AEDCB   F",
          "A   BCDEF"
        )
        testHammingDiff(
          allowSwaps,
          "ABCDEF",
          "ABCEF",
          "ABCDEF",
          "ABC EF"
        )
        testHammingDiff(
          allowSwaps,
          "ACDEBF",
          "ABCDEF",
          "A CDEBF",
          "ABCDE F"
        )
      }
    }

  }

}
