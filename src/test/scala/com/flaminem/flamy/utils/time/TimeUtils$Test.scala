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

package com.flaminem.flamy.utils.time

import org.scalatest.FunSpec

/**
  * Created by bdamour on 4/4/16.
  */
class TimeUtils$Test extends FunSpec {

  describe("TimeUtils transforms UTC to timestamp and vice-versa") {
    it("should be reversible for Universal format") {
      val actual = TimeUtils.timestampToUniversalTime(TimeUtils.universalTimeToTimeStamp("2016-03-30 15:34:27"))
      assert(actual === "2016-03-30 15:34:27")
    }

    it("should be reversible for File format") {
      val actual = TimeUtils.timestampToFileTime(TimeUtils.fileTimeToTimeStamp("2016-03-30_15.34.27"))
      assert(actual === "2016-03-30_15.34.27")
    }
  }

  describe("TimeUtils ensure all date representations are UTC") {
    it("should read timestamps in UTC") {
      assert(TimeUtils.universalTimeToTimeStamp("2016-03-30 13:34:27") === 1459344867000L)
      assert(TimeUtils.fileTimeToTimeStamp("2016-03-30_13.34.27") === 1459344867000L)
    }

    it("should position 0 at 1970-01-01 00:00:00") {
      assert(TimeUtils.timestampToUniversalTime(0L) === "1970-01-01 00:00:00")
    }
  }

}
