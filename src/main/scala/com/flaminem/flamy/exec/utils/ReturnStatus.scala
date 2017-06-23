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

/**
  * Created by fpin on 12/14/15.
  */
sealed trait ReturnStatus {
  /**
    * The value that should be returned by the program for this status
    */
  val exitCode: Int

  def isSuccess: Boolean
  def isFailure: Boolean

  def &&(that: ReturnStatus): ReturnStatus = ReturnStatus(Math.max(this.exitCode, that.exitCode))
}

object ReturnStatus {
  def apply(exitCode: Int): ReturnStatus = exitCode match {
    case 0 => ReturnSuccess
    case 1 => ReturnFailure
  }

  def apply(success: Boolean): ReturnStatus = success match {
    case true => ReturnSuccess
    case false => ReturnFailure
  }
}

case object ReturnSuccess extends ReturnStatus {
  override val exitCode: Int = 0

  override def isSuccess: Boolean = true
  override def isFailure: Boolean = false
}

case object ReturnFailure extends ReturnStatus {
  override val exitCode: Int = 1

  override def isSuccess: Boolean = false
  override def isFailure: Boolean = true
}
