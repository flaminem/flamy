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

package com.flaminem.flamy.utils.collection

import org.scalatest.FreeSpec

import scala.collection.immutable.Seq

/**
  * Created by fpin on 12/20/16.
  */
class TraversableLikeExtensionTest extends FreeSpec {

  "splitBy should correctly work with sorted groups" in {
    val l: Seq[Int] = 1 to 10
    val res = l.splitBy{x => x / 6}

    assert(res===Seq((0,Vector(1, 2, 3, 4, 5)), (1, Vector(6, 7, 8, 9, 10))))
  }

  "splitBy should correctly work with unsorted groups" in {
    val l: Seq[Int] = 1 to 10
    val res = l.splitBy{x => x % 2}

    assert(res ===
      Seq(
        (1, Vector(1)),
        (0, Vector(2)),
        (1, Vector(3)),
        (0, Vector(4)),
        (1, Vector(5)),
        (0, Vector(6)),
        (1, Vector(7)),
        (0, Vector(8)),
        (1, Vector(9)),
        (0, Vector(10))
      )
    )
  }

}
