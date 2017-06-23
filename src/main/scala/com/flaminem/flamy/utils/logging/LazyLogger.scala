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

import org.apache.logging.log4j.Logger

/**
 * A logger that only evaluates parameters lazily if the corresponding log level is enabled.
 */
sealed class LazyLogger(log: Logger) extends Serializable {

  def info(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  def debug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  def trace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  def warn(msg: => String) {
    if (log.isWarnEnabled()) log.warn(msg)
  }

  def error(msg: => String) {
    if (log.isErrorEnabled()) log.error(msg)
  }

  def info(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  def debug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  def trace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  def warn(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled()) log.warn(msg, throwable)
  }

  def error(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled()) log.error(msg, throwable)
  }
}
