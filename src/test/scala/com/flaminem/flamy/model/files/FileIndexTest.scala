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

package com.flaminem.flamy.model.files

import com.flaminem.flamy.conf.FlamyContext
import org.scalatest.FunSuite

/**
 * Created by fpin on 7/24/15.
 */
class FileIndexTest extends FunSuite {

  test("a FileIndex should work") {
    val context = new FlamyContext("flamy.model.dir.paths" -> "src/test/resources/test")
    val index = context.getFileIndex
    assert(index.size==11)
  }

}
