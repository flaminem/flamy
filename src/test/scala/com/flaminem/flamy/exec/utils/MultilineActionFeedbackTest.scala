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

package com.flaminem.flamy.exec.utils

import org.scalatest.FreeSpec

/**
  * Created by fpin on 1/9/17.
  */
class MultilineActionFeedbackTest extends FreeSpec {

  "IncrementalIndexer should work" in {
    val index = new MultilineActionFeedback.IncrementalIndexer[Int]
    assert(index(0) == 0)
    assert(index(1) == 1)
    assert(index(2) == 2)
    assert(index(0) == 0)
    assert(index(1) == 1)
    assert(index(2) == 2)
  }

}
