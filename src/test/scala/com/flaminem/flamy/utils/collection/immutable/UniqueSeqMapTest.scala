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

package com.flaminem.flamy.utils.collection.immutable

import org.scalatest.FreeSpec

/**
  * Created by fpin on 11/23/16.
  */
class UniqueSeqMapTest extends FreeSpec {

  "a UniqueSeqMap should throw an exception if the same element is inserted twice" in {
    intercept[UnicityViolationException[_]] {
      UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c", 1 -> "d")
    }
    intercept[UnicityViolationException[_]] {
      UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c") + (1 -> "d")
    }
    intercept[UnicityViolationException[_]] {
      UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c") ++ Seq(4 -> "d", 5 -> "e", 6 -> "f", 1 -> "g")
    }
    intercept[UnicityViolationException[_]] {
      UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c") ++ Seq(4 -> "d", 5 -> "e", 6 -> "f", 6 -> "g")
    }
    intercept[UnicityViolationException[_]] {
      var l = UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c")
      l += 1  -> "d"
    }
  }

  "a UniqueSeqMap should work as long as distinct keys are inserted with distinct values" in {
    assert(UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c").toSeq === Seq(1 -> "a", 2 -> "b", 3 -> "c"))
    assert((UniqueSeqMap(1 -> "a", 2 -> "b") + (3 -> "c")).toSeq === Seq(1 -> "a", 2 -> "b", 3 -> "c"))
    assert((UniqueSeqMap(1 -> "a", 2 -> "b") ++ Seq(3 -> "c", 4 -> "d")).toSeq === Seq(1 -> "a", 2 -> "b", 3 -> "c", 4 -> "d"))

    var l = UniqueSeqMap(1 -> "a", 2 -> "b")
    l += (3 -> "c")
    l ++= Seq(4 -> "d", 5 -> "e")
    assert(l.toSeq === Seq(1 -> "a", 2 -> "b", 3 -> "c", 4 -> "d", 5 -> "e"))
  }

  "a UniqueSeqMap should work as long as distinct keys are inserted with identical values" in {
    assert(UniqueSeqMap(1 -> "a", 2 -> "a", 3 -> "a").toSeq === Seq(1 -> "a", 2 -> "a", 3 -> "a"))
    assert((UniqueSeqMap(1 -> "a", 2 -> "a") + (3 -> "a")).toSeq === Seq(1 -> "a", 2 -> "a", 3 -> "a"))
    assert((UniqueSeqMap(1 -> "a", 2 -> "a") ++ Seq(3 -> "a", 4 -> "a")).toSeq === Seq(1 -> "a", 2 -> "a", 3 -> "a", 4 -> "a"))

    var l = UniqueSeqMap(1 -> "a", 2 -> "a")
    l += (3 -> "a")
    l ++= Seq(4 -> "a", 5 -> "a")
    assert(l.toSeq === Seq(1 -> "a", 2 -> "a", 3 -> "a", 4 -> "a", 5 -> "a"))
  }

  "a UniqueSeqMap should preserve ordering" in {
    assert(UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c").toSeq === Seq(1 -> "a", 2 -> "b", 3 -> "c"))
    assert(UniqueSeqMap(3 -> "a", 2 -> "b", 1 -> "c").toSeq === Seq(3 -> "a", 2 -> "b", 1 -> "c"))
    assert(UniqueSeqMap(3 -> "c", 2 -> "b", 1 -> "a").toSeq === Seq(3 -> "c", 2 -> "b", 1 -> "a"))
    assert(UniqueSeqMap(1 -> "c", 2 -> "b", 3 -> "a").toSeq === Seq(1 -> "c", 2 -> "b", 3 -> "a"))
  }

  "get should work" in {
    val m = UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c")
    assert(m.get(1) === Some("a"))
    assert(m.get(2) === Some("b"))
    assert(m.get(3) === Some("c"))
    assert(m.get(4) === None)
  }

  "getOrElse should work" in {
    val m = UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c")
    assert(m.getOrElse(1, "") === "a")
    assert(m.getOrElse(2, "") === "b")
    assert(m.getOrElse(3, "") === "c")
    assert(m.getOrElse(4, "") === "")
  }

  "filter should work" in {
    val m: Traversable[(Int, String)] =
      UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c").filter{
        case (i, c) => i % 2 == 1
      }
    assert(m === UniqueSeqMap(1 -> "a", 3 -> "c").toSeq)
  }

  "collect should work" in {
    val m: Traversable[Int] =
      UniqueSeqMap(1 -> "a", 2 -> "b", 3 -> "c").collect{
        case (i, c) if i % 2 == 1 => i
      }
    assert(m === Seq(1, 3 ))
  }

}
