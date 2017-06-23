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
  * Created by fpin on 8/18/16.
  */
class UniqueListTest extends FreeSpec {

  "a UniqueList should throw an exception if the same element is inserted twice" in {
    intercept[UnicityViolationException[_]] {
      UniqueList(1, 2, 3, 1)
    }
    intercept[UnicityViolationException[_]] {
      UniqueList(1, 2, 3) :+ 1
    }
    intercept[UnicityViolationException[_]] {
      UniqueList(1, 2, 3) ++ Seq(4, 5, 6, 1)
    }
    intercept[UnicityViolationException[_]] {
      UniqueList(1, 2, 3) ++ Seq(4, 5, 6, 6)
    }
    intercept[UnicityViolationException[_]] {
      var l = UniqueList(1, 2, 3)
      l :+= 1
    }
  }

  "a UniqueList should work as long as distinct elements are inserted " in {
    assert (UniqueList(1, 2, 3).toSeq === (1 to 3))
    assert ((UniqueList(1, 2, 3) :+ 4).toSeq === (1 to 4))
    assert ((UniqueList(1, 2, 3) ++ Seq(4,5,6)).toSeq === (1 to 6))

    var l = UniqueList(1, 2, 3)
    l :+= 4
    l ++= Seq(5, 6)
    assert(l.toSeq === (1 to 6))
  }


}
