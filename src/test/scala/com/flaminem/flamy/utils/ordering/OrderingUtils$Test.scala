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

package com.flaminem.flamy.utils.ordering

import org.scalatest.FreeSpec

/**
  * Created by fpin on 8/22/16.
  */
class OrderingUtils$Test extends FreeSpec {

  def failingInt : Int = {
    throw new Exception()
  }

  "Methods tests" - {
    "firstNonZero  (2 args)" in {
      assert(OrderingUtils.firstNonZero(0, 0) === 0)
      assert(OrderingUtils.firstNonZero(1, 2) === 1)
      assert(OrderingUtils.firstNonZero(0, 2) === 2)
      assert(OrderingUtils.firstNonZero(1, failingInt) === 1)
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt)
      }
    }

    "firstNonZero  (3 args)" in {
      assert(OrderingUtils.firstNonZero(0, 0, 0) === 0)
      assert(OrderingUtils.firstNonZero(0, 1, 2) === 1)
      assert(OrderingUtils.firstNonZero(0, 0, 2) === 2)
      assert(OrderingUtils.firstNonZero(1, 2, 3) === 1)
      assert(OrderingUtils.firstNonZero(1, failingInt, failingInt) === 1)
      assert(OrderingUtils.firstNonZero(0, 1, failingInt) === 1)
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, failingInt)
      }
    }

    "firstNonZero  (4 args)" in {
      assert(OrderingUtils.firstNonZero(0, 0, 0, 0) === 0)
      assert(OrderingUtils.firstNonZero(0, 1, 2, 3) === 1)
      assert(OrderingUtils.firstNonZero(0, 0, 2, 3) === 2)
      assert(OrderingUtils.firstNonZero(0, 0, 0, 3) === 3)
      assert(OrderingUtils.firstNonZero(1, failingInt, failingInt, failingInt) === 1)
      assert(OrderingUtils.firstNonZero(0, 1, failingInt, failingInt) === 1)
      assert(OrderingUtils.firstNonZero(0, 0, 1, failingInt) === 1)
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, failingInt, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, failingInt, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, 0, failingInt)
      }
    }

    "firstNonZero  (5 args)" in {
      assert(OrderingUtils.firstNonZero(0, 0, 0, 0, 0) === 0)
      assert(OrderingUtils.firstNonZero(0, 1, 2, 3, 4) === 1)
      assert(OrderingUtils.firstNonZero(0, 0, 2, 3, 4) === 2)
      assert(OrderingUtils.firstNonZero(0, 0, 0, 3, 4) === 3)
      assert(OrderingUtils.firstNonZero(0, 0, 0, 0, 4) === 4)
      assert(OrderingUtils.firstNonZero(1, failingInt, failingInt, failingInt, failingInt) === 1)
      assert(OrderingUtils.firstNonZero(0, 1, failingInt, failingInt, failingInt) === 1)
      assert(OrderingUtils.firstNonZero(0, 0, 1, failingInt, failingInt) === 1)
      assert(OrderingUtils.firstNonZero(0, 0, 0, 1, failingInt) === 1)
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 0, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 0, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 0, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 0, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 1, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 1, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 1, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 0, 1, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 0, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 0, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 0, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 0, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 1, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 1, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 1, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(failingInt, 1, 1, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 0, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 0, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 0, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 0, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 1, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 1, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 1, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, failingInt, 1, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, failingInt, 0, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, failingInt, 0, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, failingInt, 1, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, failingInt, 1, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, 0, failingInt, 0)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, 0, failingInt, 1)
      }
      intercept[Exception]{
        OrderingUtils.firstNonZero(0, 0, 0, 0, failingInt)
      }
    }

  }
}
