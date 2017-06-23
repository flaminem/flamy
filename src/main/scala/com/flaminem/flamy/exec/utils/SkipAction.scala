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

import com.flaminem.flamy.model.exceptions.UnexpectedBehaviorException

/**
  * A special type of Action that needs to be skipped.
  * This allows to create actions that should not be executed, but may have side effects
  * after being skipped.
  */
trait SkipAction extends Action {

  /**
    * SkipActions should never be executed, always skipped.
    */
  override def run() {
    throw new UnexpectedBehaviorException()
  }

}
