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

import com.typesafe.config.Config
import org.apache.commons.configuration.Configuration

/**
  * These configuration properties are global and accessible from the whole JVM.
  * The configuration can be reloaded with [[FlamyGlobalContext.init]], but only one instance can exist at a given time.
  */
object FlamyGlobalContext extends FlamyGlobalConfVars {

  private var logFolderPath : String = "run/log"

  def setLogFolder(path: String): Unit = {
    this.logFolderPath = path
  }

  /**
    * Load or reload the global configuration
    */
  def init(conf: Config): Unit = {
    this.conf = conf
    confVars.foreach{_.resetCachedValue()}
    setLogFolder(getRunDir + "/logs")
  }

  def getLogFolder: String = logFolderPath

  def getRunDir: String = {
    RUN_DIR.getProperty + "/" + FlamyContext.processId
  }

}
