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

package com.flaminem.flamy.graph.export

import org.scalatest.FreeSpec

/**
  * Created by fpin on 12/5/16.
  */
class AlignmentTest extends FreeSpec {

  "toString must be correct" in {
    assert(Alignment.CENTER.toString == "CENTER")
    assert(Alignment.LEFT.toString == "LEFT")
    assert(Alignment.RIGHT.toString == "RIGHT")
  }


}
