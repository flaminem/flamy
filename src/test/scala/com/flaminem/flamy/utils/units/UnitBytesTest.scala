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

package com.flaminem.flamy.utils.units


import org.scalatest.FunSuite

/**
 * Created by fpin on 12/17/14.
 */
class UnitBytesTest extends FunSuite {

  test("Bytes should be correctly displayed") {
    assert(UnitBytes(0L).toString==="0 b")
    assert(UnitBytes(1L).toString==="1 b")
    assert(UnitBytes(1024L).toString==="1 Kb")
    assert(UnitBytes(10L*1024).toString==="10 Kb")
    assert(UnitBytes(666L*1024).toString==="666 Kb")
    assert(UnitBytes(1024L*1024).toString==="1 Mb")
    assert(UnitBytes(10L*1024*1024).toString==="10 Mb")
    assert(UnitBytes(666L*1024*1024).toString==="666 Mb")
    assert(UnitBytes(1024L*1024*1024).toString==="1 Gb")
    assert(UnitBytes(10L*1024*1024*1024).toString==="10 Gb")
    assert(UnitBytes(666L*1024*1024*1024).toString==="666 Gb")
    assert(UnitBytes(1024L*1024*1024*1024).toString==="1 Tb")
    assert(UnitBytes(10L*1024*1024*1024*1024).toString==="10 Tb")
    assert(UnitBytes(666L*1024*1024*1024*1024).toString==="666 Tb")
    assert(UnitBytes(1024L*1024*1024*1024*1024).toString==="1 Pb")
    assert(UnitBytes(10L*1024*1024*1024*1024*1024).toString==="10 Pb")
    assert(UnitBytes(666L*1024*1024*1024*1024*1024).toString==="666 Pb")
  }


}
