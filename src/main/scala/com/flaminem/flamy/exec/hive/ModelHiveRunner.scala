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

package com.flaminem.flamy.exec.hive

import com.flaminem.flamy.conf.FlamyContext
import com.flaminem.flamy.model.Variables
import com.flaminem.flamy.utils.logging.Logging

/**
 * Created by fpin on 2/17/15.
 */
class ModelHiveRunner(override val context: FlamyContext) extends LocalHiveRunner(context: FlamyContext) with Logging {

  override def runCreate(text: String, variables: Variables): Unit = {
    runText(ModelHiveRunner.removeLocation(text), None, variables, explainIfDryRun = false)
  }

}

object ModelHiveRunner {

  private[hive] def removeLocation(text: String): String = {
    text.replaceAll("""(?i)location\s+(['"])([^\\]|\\.)*?\1""", "")
  }

}