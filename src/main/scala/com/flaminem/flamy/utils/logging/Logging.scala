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

package com.flaminem.flamy.utils.logging

import org.apache.logging.log4j.LogManager

/**
 * Created by fpin on 12/2/14.
 *
 * Shamelessly pumped from:
 * https://github.com/scalanlp/breeze/blob/master/math/src/main/scala/breeze/util/SerializableLogging.scala
 * "Stupid Typesafe logging lib trait isn't serializable. This is just a better version."
 */
trait Logging extends Serializable {
  @transient
  protected lazy val logger : LazyLogger = new LazyLogger(LogManager.getLogger(this.getClass))

  def logInfo(s: =>String): Unit = logger.info(s)
  def logInfo(s: =>String, e:Throwable): Unit = logger.info(s,e)

  def logWarning(s: =>String): Unit = logger.warn(s)
  def logWarning(s: =>String, e:Throwable): Unit = logger.warn(s,e)

  def logError(s:String): Unit = logger.error(s)
  def logError(s:String, e:Throwable): Unit = logger.error(s,e)

  def logDebug(s:String): Unit = logger.debug(s)
  def logDebug(s:String, e:Throwable): Unit = logger.debug(s,e)

}


