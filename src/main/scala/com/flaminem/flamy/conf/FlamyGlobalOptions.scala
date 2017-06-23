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

package com.flaminem.flamy.conf

import com.flaminem.flamy.model.Variables

/**
 * This contains all the global options given through the CLI to Flamy.
 */
class FlamyGlobalOptions (
  val variables: Variables = Variables(),
  val conf: Map[String,String] = Map(),
  val configFile: Option[String] = None
) {

  /**
    * Return a copy of these options, merged with the set of options given as argument.
    * In case of collision, the latter is kept.
    * @param that
    * @return
    */
  def overrideWith(that: FlamyGlobalOptions): FlamyGlobalOptions = {
    new FlamyGlobalOptions(
      Variables((this.variables ++ that.variables).toSeq:_*),
      this.conf ++ that.conf,
      that.configFile.orElse(this.configFile)
    )
  }

}
