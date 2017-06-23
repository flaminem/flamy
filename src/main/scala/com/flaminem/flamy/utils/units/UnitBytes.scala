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

/**
 * Created by fpin on 12/17/14.
 */
object UnitBytes{
  val Scale = 1024
  val prefixes = List("","K","M","G","T","P")
  val unit = "b"
}

case class UnitBytes(value: Long) {


  private def autoScale(v: Long, prefixes: List[String] = UnitBytes.prefixes): String = {
    (v, prefixes) match {
      case (0, _) => "0 "
      case (x, t :: Nil) => x + " " + t
      case (x, t :: q) =>
        x / UnitBytes.Scale match {
          case 0 => x + " " + t
          case y => autoScale(y, q)
        }
      case (x, Nil) => x + " "
    }
  }

  override def toString: String = {
    autoScale(value) + UnitBytes.unit
  }


}

