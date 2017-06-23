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

import org.scalatest._

/**
 * Created by fpin on 12/16/14.
 */
class PairTraversableLikeExtensionTest extends FlatSpec with Matchers {

  "groupByKey" should "correctly work" in {
    val l = (1 to 10).map{x => (x/2,x)}.groupByKey.toList
    assert(l===List((0,Vector(1)), (5,Vector(10)), (1,Vector(2, 3)), (2,Vector(4, 5)), (3,Vector(6, 7)), (4,Vector(8, 9))))
  }

  "groupBySortedKey" should "correctly work" in {
    val l = (1 to 10).map{x => (x/2,x)}.groupBySortedKey.toList
    assert(l===List((0,Vector(1)), (1,Vector(2, 3)), (2,Vector(4, 5)), (3,Vector(6, 7)), (4,Vector(8, 9)), (5,Vector(10))))
  }

  "sortByKey" should "correctly work" in {
    val m: Map[Int, Int] = Map.empty + ((1, 1), (3, 3), (2, 2))
    assert(m.sortByKey.toMap===m)
    assert(m.sortByKey.toMap.toList!=m.toList)
  }


}
