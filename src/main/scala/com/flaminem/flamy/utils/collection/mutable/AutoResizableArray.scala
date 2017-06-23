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

package com.flaminem.flamy.utils.collection.mutable

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A simple ArrayBuffer, that automatically scale when inserting a value at an out-of-bound position
  *
  * @param defaultValue
  * @tparam T
  */
// TODO: unit test this
class AutoResizableArray[T](defaultValue: T) {

  private val buf: ArrayBuffer[T] = mutable.ArrayBuffer[T]()

  def update(index: Int, value: T): Unit = {
    if(index < 0){
      throw new IndexOutOfBoundsException(index.toString)
    }
    if(index >= buf.size) {
      buf ++= (buf.size to index).map{_ => defaultValue}
    }
    buf(index) = value
  }

  def apply(index: Int): T ={
    if(index >= buf.size){
      defaultValue
    }
    else{
      buf(index)
    }
  }

  def slice(from: Int, until: Int): Seq[T] = {
    buf.slice(from, until).toList
  }

  def size: Int = {
    buf.size
  }

  override def toString: String = {
    buf.toString()
  }
}
