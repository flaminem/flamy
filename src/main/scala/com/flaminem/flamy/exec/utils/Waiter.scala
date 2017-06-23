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

import org.apache.commons.lang.time.StopWatch

/**
  * Created by fpin on 12/2/14.
  */
object Waiter {

  /**
    * Waits for an event to become true, unless the timeout (in seconds) is reached first.
    * Sleeps  (period in milliseconds).
    * The event should not be given as a constant ('val') but as a function ('def').
    *
    *
    * @param event    that we wait for
    * @param timeOut  in seconds
    * @param sleepTime in seconds
    * @return
    */
  def waitForEvent(event: => Boolean, timeOut: Long, sleepTime: Long): Boolean = {
    val stopWatch = new StopWatch
    stopWatch.start()
    while (stopWatch.getTime < timeOut * 1000) {
      if (event) {
        return true
      }
      Thread.sleep(sleepTime * 1000)
    }
    false
  }


}
